"""
Helpers for processing sqlite databases
"""

from __future__ import annotations

import re
import shutil
import sqlite3
import subprocess
from collections.abc import Iterator, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
from sqlite3 import Connection
from typing import Literal

from ..processor import (
    BaseNormaliser,
    Normalised,
    sort_file,
    unique_file_in_tempdir,
)

AllowedBlobs = frozenset[tuple[str, str]]


_SQLITE_HEX_BLOB_PREFIX = b"X'"
_JSON_OBJECT_START_HEX = f'{ord("{"):02x}'.encode()
_JSON_OBJECT_END_HEX = f'{ord("}"):02x}'.encode()
_JSON_OBJECT_HEX_CANDIDATE = _SQLITE_HEX_BLOB_PREFIX + _JSON_OBJECT_START_HEX[:1]
# SQLite dumps BLOBs as X'ABCD'. JSON objects start with "{" (0x7b) and end with "}" (0x7d), so only those blobs need this readability rewrite.
_JSON_HEX_BLOB_RE = re.compile(
    rb"X'(" + _JSON_OBJECT_START_HEX + rb"[0-9a-f]*" + _JSON_OBJECT_END_HEX + rb")'",
    re.IGNORECASE,  # sqlite hex output can be both upper and lower case
)
_MALFORMED_FTS_CHECK_PREFIX = 'malformed inverted index for FTS'


def _postprocess_dump_hex_bytes(data: bytes) -> bytes:
    if _JSON_OBJECT_HEX_CANDIDATE not in data:
        # Coarse fast path: the first hex digit of "{" is enough to skip dumps with no JSON-ish blobs; the regex below checks the full 7b...7d shape.
        return data

    def replace(match: re.Match[bytes]) -> bytes:
        ss = bytes.fromhex(match.group(1).decode())
        # Keep one dump record per line, otherwise sorting/diffing the dump gets ambiguous.
        ss = re.sub(rb'(\r\n|\r|\n)', b'<NEWLINE>', ss)
        return b"X'" + ss + b"'"

    return _JSON_HEX_BLOB_RE.sub(replace, data)


def _postprocess_dump_hex_line(line: bytes) -> bytes:
    return _postprocess_dump_hex_bytes(line)


def _postprocess_dump_hex(*, src: Path, dst: Path) -> Path:
    data = src.read_bytes()
    processed = _postprocess_dump_hex_bytes(data)
    if processed == data:
        # if hex processing had no effect, no need to write out files (can waste hundreds of ms of time/disk IO)
        # just reuse the src
        return src

    dst.write_bytes(processed)
    shutil.move(dst, src)
    return src


def _checked_no_wal(db: Path) -> Path:
    shm = db.parent / (db.name + '-shm')
    wal = db.parent / (db.name + '-wal')
    assert not shm.exists(), shm
    assert not wal.exists(), wal
    return db


# Ok, so TLDR: sometimes sqlite might dump blob data as empty strings.
# This obviously may result in inconsistent data view, and me might prune too much data.
#
# Essentially this happens because sqlite is relaxed about actual types of the data inserted
# (unless the tables were created as STRICT in the first place).
# In fact, sqlite3 .dump command doesn't even look at the schema at all, it always relies on the sqlite "cell" type.
# See https://github.com/sqlite/sqlite/blob/0b4de1acac7da83cfaf72cbd00d1d1f2fd456b1a/ext/misc/dbdump.c#L481
#
# The really problematic case is when a TEXT value was inserted in the column that is supposed to be a BLOB.
# In this case, sqlite3 .dump sometimes just ends up writing the blob as empty string.
# This doesn't happen always, but for instance does if the blob starts with zero bytes
# (supposedly sqlite C code treats it as null terminator then??)
#
# As a workaround, here we are checking that BLOB columns only actually contain BLOB values.
# If this is the case, sqlite will properly dump the blob as hex with X prepended to it.
# Otherwise, we check allowed_blobs from configs, which is essentially an 'ignore list' for such 'bad' BLOB columns.
# If the column isn't in the ignore list, we just error since it would be unsafe to compare such databases.
#
# This logic is tested to some extent by tests/sqlite.py::test_sqlite_blobs_allowed
def _check_allowed_blobs(*, conn: Connection, allowed_blobs: AllowedBlobs) -> None:
    tool = Tool(conn)
    schemas = tool.get_tables()
    bad_blobs = []
    for table, schema in schemas.items():
        for col, type_ in schema.items():
            if type_ != 'BLOB':
                continue
            key     = (table, col)  # fmt: skip
            any_key = (table, '*')
            if (key in allowed_blobs) or (any_key in allowed_blobs):
                continue
            actual_types: set[str] = {at for (at,) in conn.execute(f'SELECT DISTINCT typeof(`{col}`) FROM `{table}`')}
            actual_types.discard('null')  # nulls are harmless, worst case dumped as empty string

            if actual_types == {'blob'}:
                # OK, schema says blob, and the recorded type is blob -- it'll always be dumped correctly
                continue

            if actual_types == set():
                # table has no actual data -- fine as well
                continue

            bad_blobs.append((key, actual_types))

    if len(bad_blobs) > 0:
        raise RuntimeError(
            '\n'.join(
                f"{key} : has type BLOB but contains values of other types ({actual_types}). "
                "This may result in wrong textual representation for the database and pruning files we shouldn't prune. "
                "Consider adding this to ALLOWED_BLOBS or removing the corresponding table from the db if you think it's safe to ignore."
                for key, actual_types in bad_blobs
            )
        )


def _checked_db(
    db: Path,
    *,
    check: Literal['integrity', 'quick'],
    allowed_blobs: AllowedBlobs | None,
    custom_tokenizers: frozenset[str] = frozenset(),
) -> Path:
    """
    check: 'integrity' can be quite slow, O(N log N) in number of rows, because it checks all indices and UNIQUE constraints.
           'quick' is O(N), and mostly achieves same result.
    """
    db = _checked_no_wal(db)
    # NOTE: with immutable=1, SQLite can skip some checks; e.g. integrity_check can return ok even if a normal
    # connection reports CHECK constraint violations. Here this is mostly a cheap readonly sanity check.
    with closing(sqlite3.connect(f'file:{db}?immutable=1', uri=True)) as conn, conn:
        # note: .execute only does statement at a time?
        # TODO what does schema_version do?
        list(conn.execute('PRAGMA schema_version;'))
        try:
            check_results = [r for (r,) in conn.execute(f'PRAGMA {check}_check;')]
        except sqlite3.OperationalError as e:
            unknown_tokenizer_errors = {f'unknown tokenizer: {tokenizer}' for tokenizer in custom_tokenizers}
            if str(e) not in unknown_tokenizer_errors:
                raise
            # A custom tokenizer can make quick_check unusable before dumben removes virtual tables.
            # The copied, dumbed-down database still receives a full integrity check below.
            assert check == 'quick', check
            check_results = []
        # PRAGMA *_check returns one row per problem, or a single "ok" row. Ignore malformed FTS indexes: they are derived search data and dumben strips virtual tables anyway. Seen with PodcastAddict.
        bad_results = [r for r in check_results if r != 'ok' and not r.startswith(_MALFORMED_FTS_CHECK_PREFIX)]
        assert len(bad_results) == 0, '\n'.join(bad_results)
        if allowed_blobs is not None:
            _check_allowed_blobs(conn=conn, allowed_blobs=allowed_blobs)

    db = _checked_no_wal(db)
    return db


class SqliteNormaliser(BaseNormaliser):
    # FIXME need a test, i.e. with removing single row?

    ALLOWED_BLOBS: AllowedBlobs = frozenset()

    CUSTOM_TOKENIZERS: frozenset[str] = frozenset()
    """
    Names of application-defined FTS tokenizers that may be unavailable in the local SQLite build.

    An exact ``unknown tokenizer`` error for one of these names is allowed only during the original database's quick check.
    Dumben then removes virtual tables from a private copy, which still receives full integrity and BLOB checks.
    """

    # TODO in principle we can get away with using only 'extract'?
    # 'cleanup' is just a sanity check? so you don't cleanup too much by accident?
    # guess it makes it easier to specify only one of them?
    # - by default, cleanup doesn't do anything
    # - by default, extract extracts everything
    # TODO needs to return if they are same or dominated?
    # for BM it's fine to prune delete dominated though..
    # except... need to keep filenames? this could be useful info...
    # need to decide where to log them...

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Normalised]:
        # note: deliberately keeping mime check inside do_cleanup, since it's executed in a parallel process
        # ok, this is much easier than detecting mime or whatever...
        with path.open('rb') as fb:
            header = fb.read(16)
            assert header == b"SQLite format 3\x00", header

        # TODO handle compressed databases later... need to think how to work around checking for no wal etc..
        upath = path
        del path  # just to prevent from using by accident

        # first, do not check for blobs -- we might not even be able to get the table list in python due to virtual tables
        # NOTE: quick check (instead of integrity) is fine here -- we're going to drop all indices during dumben step anyway,
        #   and it does introduce substantial speedup for bigger databases (e.g. browser history).
        upath = _checked_db(
            upath,
            check='quick',
            allowed_blobs=None,
            custom_tokenizers=self.CUSTOM_TOKENIZERS,
        )
        # NOTE: upath here is still the _original_  path passed to bleanser, so we can't modify in place

        assert upath.is_absolute(), f'{upath} is not an absolute path'

        cleaned_db = unique_file_in_tempdir(input_filepath=upath, dir=self.tmp_dir, suffix='.db')
        unique_tmp_dir = cleaned_db.parent

        from bleanser.core.ext.sqlite_dumben import run as dumben

        dumben(db=upath, output=cleaned_db, output_as_db=True)

        # eh.. not sure if really necessary
        # but we don't wanna check for blobs yet, better to do this after the cleanup
        cleaned_db = _checked_db(cleaned_db, check='integrity', allowed_blobs=None)

        # ugh. in principle could use :memory: database here...
        # but then dumping it via iterdump() takes much more time then sqlite3 .dump command..
        with closing(sqlite3.connect(cleaned_db)) as conn, conn:
            # prevent it from generating unnecessary wal files
            conn.execute('PRAGMA journal_mode=MEMORY;')

            # extra paranoid checks...
            # TODO maybe also get create statements from sqlite_master and assert no constraints etc
            # and double check it by passing something without dumbing down
            tool = Tool(conn)
            master_info = tool.get_sqlite_master()
            assert all(x == 'table' for x in master_info.values()), master_info
            # TODO how to check there are no more triggers etc for real? do we need to commit or smth?

            # cleanup might take a bit of time, especially with UPDATE statements
            # but probably unavoidable?
            self.cleanup(conn)
        # FIXME ugh annoying -- conn/tool can hold a reference to connection, so despite closing might hold the reference to the file (even though it's unlinked)
        # this can result in running out of file descriptors
        # really need to cover the whole things with tests more and then refactor...
        del tool
        del conn

        cleaned_db = _checked_db(cleaned_db, check='integrity', allowed_blobs=self.ALLOWED_BLOBS)

        ### dump to text file
        ## prepare a fake path for dump, just to preserve original file paths at least to some extent
        dump_file = unique_tmp_dir / 'dump.sql'

        # dumping also takes a bit of time for big databases...
        with dump_file.open('wb') as fo:
            subprocess.check_call(['sqlite3', '-readonly', f'file://{cleaned_db}?immutable=1', '.dump'], stdout=fo)

        ## one issue is that .dump dumps sometimes text columns as hex-encoded and prefixed with X
        ## this makes sense if you're using .dump output to create another db
        ## but in our case makes diffs very cryptic
        dump_file_nohex = unique_tmp_dir / 'dump_nohex.sql'
        # TODO hmm this might break if it's a legit binary BLOB?
        # TODO maybe only do it in diff mode? not sure
        dump_file = _postprocess_dump_hex(src=dump_file, dst=dump_file_nohex)
        ##

        # alternative way to dump database
        # could be useful when you have multiline strings or jsons in TEXT/STRING fields
        # in this case sqlite .dump prepends them with X and encodes
        # however, this makes it much harder to spot differences
        # if we ever use it this way, this should
        # - pass a custom -newline to sqlite (e.g. \0)
        # - replace \n in output with space or something
        # - replace the -newline symbol with actual \n
        # for table in master_info:
        #     query_cmd = sqlite_cmd['-readonly', f'file://{cleaned_db}?immutable=1', f'SELECT "{table}", * FROM `{table}`']
        #     cmd = query_cmd >> str(dump_file)
        #     cmd()

        # hmm seems necessary sometimes.. not sure why
        sort_file(dump_file)

        cleaned_db.unlink()
        ###
        yield dump_file

    def cleanup(self, c: Connection) -> None:
        pass


class Tool:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def get_sqlite_master(self) -> dict[str, str]:
        res = {}
        for c in self.connection.execute('SELECT name, type FROM sqlite_master'):
            [name, type_] = c
            assert type_ in {'table', 'index', 'view', 'trigger'}, (name, type_)  # just in case
            res[name] = type_
        return res

    def get_tables(self) -> dict[str, dict[str, str]]:
        sm = self.get_sqlite_master()

        res: dict[str, dict[str, str]] = {}
        for name, type_ in sm.items():
            if type_ != 'table':
                continue
            schema: dict[str, str] = {}
            for row in self.connection.execute(f'PRAGMA table_info(`{name}`)'):
                col = row[1]
                type_ = row[2]
                # hmm, somewhere between 3.34.1 and 3.37.2, sqlite started normalising type names to uppercase
                # let's do this just in case since python < 3.10 are using the old version
                # e.g. it could have returned 'blob' and that would confuse blob check (see _check_allowed_blobs)
                type_ = type_.upper()
                schema[col] = type_
            res[name] = schema
        return res

    def count(self, table: str) -> int:
        [(res,)] = self.connection.execute(f'SELECT COUNT(*) FROM `{table}`')
        return res

    def drop(self, table: str, *tables: str) -> None:
        # NOTE: both table and tables aregs are for backwards compat..
        all_tables = [table, *tables]
        for tbl in all_tables:
            self.connection.execute(f'DROP TABLE IF EXISTS `{tbl}`')

    def drop_view(self, view: str) -> None:
        self.connection.execute(f'DROP VIEW IF EXISTS `{view}`')

    def drop_index(self, index: str) -> None:
        self.connection.execute(f'DROP INDEX IF EXISTS `{index}`')

    def update(self, table: str, **kwargs) -> None:
        # note: seems that can't parameterize col name in sqlite
        kws = ', '.join(f'`{k}`=?' for k, v in kwargs.items())
        self.connection.execute(f'UPDATE {table} SET {kws}', list(kwargs.values()))

    def drop_cols(self, table: str, *, cols: Sequence[str]) -> None:
        # for the purposes of comparison this is same as dropping
        # for update need to filter nonexisting cols
        #
        cur = self.connection.execute(f'PRAGMA table_info(`{table}`)')
        existing = [r[1] for r in cur]
        # todo warn maybe if dropped columns?
        cols = [c for c in cols if c in existing]
        if len(cols) == 0:
            return
        self.update(table, **dict.fromkeys(cols, None))
        # TODO crap. https://stackoverflow.com/a/66399224/706389
        # alter table is since march 2021... so won't be in sqlite for a while
        # TODO hmm it actually works a bit slower? weird
        # for col in cols:
        #     self.connection.execute(f'ALTER TABLE {table} DROP COLUMN {col}')

    def fix_bad_blob_column(self, table: str, *, column: str) -> None:
        # see _check_allowed_blobs for more context and docs
        db_schema = self.get_tables()
        table_schema = db_schema.get(table)
        if table_schema is None:
            return
        column_type = table_schema.get(column)
        if column_type is None:
            return
        assert column_type == 'BLOB', column_type

        actual_types: set[str] = {
            at for (at,) in self.connection.execute(f'SELECT DISTINCT typeof(`{column}`) FROM `{table}`')
        }
        actual_types.discard('null')

        if actual_types == {'blob'}:
            return

        if actual_types == set():
            # table has no actual data -- fine as well
            return

        # just in case, assuming the most common issue is when strings are kept as blobs
        assert actual_types == {'text'}, actual_types

        self.connection.execute(f'UPDATE `{table}` SET `{column}` = CAST(`{column}` AS BLOB)')


if __name__ == '__main__':
    SqliteNormaliser.main()
