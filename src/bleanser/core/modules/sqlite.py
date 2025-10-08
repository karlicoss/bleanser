"""
Helpers for processing sqlite databases
"""

from __future__ import annotations

import re
import shutil
import sqlite3
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from sqlite3 import Connection
from typing import Any

from plumbum import local  # type: ignore[import-untyped]

from ..common import Keep, Prune, parametrize
from ..processor import (
    BaseNormaliser,
    Normalised,
    compute_groups,
    compute_instructions,
    sort_file,
    unique_file_in_tempdir,
)
from ..utils import mime

AllowedBlobs = frozenset[tuple[str, str]]


def checked_no_wal(db: Path) -> Path:
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


def checked_db(db: Path, *, allowed_blobs: AllowedBlobs | None) -> Path:
    # integrity check
    db = checked_no_wal(db)
    with sqlite3.connect(f'file:{db}?immutable=1', uri=True) as conn:
        # note: .execute only does statement at a time?
        list(conn.execute('PRAGMA schema_version;'))
        list(conn.execute('PRAGMA integrity_check;'))
        if allowed_blobs is not None:
            _check_allowed_blobs(conn=conn, allowed_blobs=allowed_blobs)

    conn.close()
    db = checked_no_wal(db)
    return db


grep = local['grep']
sqlite_cmd = local['sqlite3']


def _dict2db(d: dict, *, to: Path) -> Path:
    with sqlite3.connect(to) as conn:
        for table_name, rows in d.items():
            schema = rows[0]
            s = ', '.join(schema)
            qq = ', '.join('?' for _ in schema)
            conn.execute(f'CREATE TABLE `{table_name}` ({s})')
            conn.executemany(f'INSERT INTO `{table_name}` VALUES ({qq})', rows[1:])
    conn.close()
    return to  # just for convenience


def test_sqlite_simple(tmp_path: Path) -> None:
    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = False
        PRUNE_DOMINATED = True

    func = lambda paths: compute_groups(paths, Normaliser=TestNormaliser)

    d: dict[str, Any] = {'tq': [['col1', 'col2']]}
    ### just one file
    db1 = _dict2db(d, to=tmp_path / '1.db')
    [g11] = func([db1])
    assert g11.items  == [db1]  # fmt: skip
    assert g11.pivots == [db1]
    ###

    ### simple 'dominates' test
    d['t1'] = [
        ['col1', 'col2'],
        [1     , 2     ],
        [3     , 4     ],
    ]  # fmt: skip
    db2 = _dict2db(d, to=tmp_path / '2.db')

    [g21] = func([db1, db2])
    assert g21.items  == [db1, db2]  # fmt: skip
    assert g21.pivots == [db1, db2]
    ###

    ### test error handling
    db3 = tmp_path / '3.db'
    db3.write_text('BAD')

    # TODO wow it's really messy...
    # fmt: off
    [x1, x2] = func([db3, db1, db2])
    assert x1.items  == [db3]
    assert x1.pivots == [db3]
    assert x2.items  == [db1, db2]
    assert x2.pivots == [db1, db2]

    [g31, g32, g33] = func([db1, db2, db3])
    assert g31 == g21
    assert g32.items  == [db2]
    assert g32.pivots == [db2]
    assert g33.items  == [db3]
    assert g33.pivots == [db3]
    # fmt: on
    # FIXME check error reason
    ###

    ### test 'same' handling
    db4 = _dict2db(d, to=tmp_path / '4.db')
    db5 = _dict2db(d, to=tmp_path / '5.db')
    db6 = _dict2db(d, to=tmp_path / '6.db')

    dbs = [db1, db2, db3, db4, db5, db6]
    [_g41, _g42, g43, g44] = func(dbs)
    assert g43 == g33
    assert g44.items  == [db4, db5, db6]  # fmt: skip
    assert g44.pivots == [db4,      db6]  # fmt: skip
    ###

    instrs = compute_instructions(
        dbs,
        Normaliser=SqliteNormaliser,
        threads=None,
    )
    assert list(map(type, instrs)) == [
        Keep,   # 1
        Keep,   # 2
        Keep,   # 3
        Keep,   # 4, keep the boundary
        Prune,  # 5
        Keep,   # 6, keep the boundary
    ]  # fmt: skip

    ### test when stuff was removed
    del d['t1'][-1]
    db7 = _dict2db(d, to=tmp_path / '7.db')
    dbs = [db1, db2, db3, db4, db5, db6, db7]
    [_, _, _, g54, g55, g56] = func(dbs)
    assert g54 == g44
    # TODO ugh. this is confusing... why it emits more pivots?
    assert g55.items  == [db6]  # fmt: skip
    assert g55.pivots == [db6]
    assert g56.items  == [db7]  # fmt: skip
    assert g56.pivots == [db7]
    ###

    instrs = compute_instructions(
        dbs,
        Normaliser=SqliteNormaliser,
        threads=None,
    )
    assert list(map(type, instrs)) == [
        Keep,   # 1
        Keep,   # 2
        Keep,   # 3
        Keep,   # 4, keep the boundary
        Prune,  # 5
        Keep,   # 6, keep the boundary
        Keep,   # 7,
    ]  # fmt: skip


@parametrize('multiway', [False, True])
def test_sqlite_many(*, tmp_path: Path, multiway: bool) -> None:
    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = multiway
        PRUNE_DOMINATED = True

    N = 2000

    paths = []
    d: dict[str, Any] = {}
    for i in range(N):
        if i % 10 == 0:
            # flush so sometimes it emits groups
            d = {'t': [('number',)]}
        d['t'].append((i,))
        p = _dict2db(d, to=tmp_path / f'{i:04}.db')
        paths.append(p)

    # shouldn't crash
    _instructions = list(
        compute_instructions(
            paths,
            Normaliser=TestNormaliser,
            threads=None,
        )
    )


# TODO add some tests for my own dbs? e.g. stashed


class SqliteNormaliser(BaseNormaliser):
    # FIXME need a test, i.e. with removing single row?

    ALLOWED_BLOBS: AllowedBlobs = frozenset()

    @classmethod
    def checked(cls, db: Path) -> Path:
        """common schema checks (for both cleanup/extract)"""
        return checked_db(db, allowed_blobs=cls.ALLOWED_BLOBS)

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
        # otherwise it essentially blocks waiting for all mimes to compute..
        mp = mime(path)
        assert mp in {
            'application/x-sqlite3',
            'application/vnd.sqlite3',
        }, mp
        ##

        # TODO handle compressed databases later... need to think how to work around checking for no wal etc..
        upath = path
        del path  # just to prevent from using by accident

        # first, do not check for blobs -- we might not even be able to get the table list in python due to virtual tables
        upath = checked_db(upath, allowed_blobs=None)
        # NOTE: upath here is still the _original_  path passed to bleanser, so we can't modify in place

        assert upath.is_absolute(), f'{upath} is not an absolute path'

        cleaned_db = unique_file_in_tempdir(input_filepath=upath, dir=self.tmp_dir, suffix='.db')
        unique_tmp_dir = cleaned_db.parent

        from bleanser.core.ext.sqlite_dumben import run as dumben

        dumben(db=upath, output=cleaned_db, output_as_db=True)

        # eh.. not sure if really necessary
        # but we don't wanna check for blobs yet, better to do this after the cleanup
        cleaned_db = checked_db(cleaned_db, allowed_blobs=None)

        # ugh. in principle could use :memory: database here...
        # but then dumping it via iterdump() takes much more time then sqlite3 .dump command..
        with sqlite3.connect(cleaned_db) as conn:
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

            # for possible later use
            master_info = tool.get_sqlite_master()
        conn.close()
        cleaned_db = self.checked(cleaned_db)

        ### dump to text file
        ## prepare a fake path for dump, just to preserve original file paths at least to some extent
        dump_file = unique_tmp_dir / 'dump.sql'

        # dumping also takes a bit of time for big databases...
        dump_cmd = sqlite_cmd['-readonly', f'file://{cleaned_db}?immutable=1', '.dump']
        cmd = dump_cmd > str(dump_file)
        cmd()

        ## one issue is that .dump dumps sometimes text columns as hex-encoded and prefixed with X
        ## this makes sense if you're using .dump output to create another db
        ## but in our case makes diffs very cryptic
        dump_file_nohex = unique_tmp_dir / 'dump_nohex.sql'
        # TODO hmm this might break if it's a legit binary BLOB?
        with dump_file.open('rb') as fi, dump_file_nohex.open('wb') as fo:
            for line in fi:
                assert line.endswith(b'\n')
                # fixme need to find all in case of multiple hex columns
                m = re.search(b"X'([0-9a-f]*)'", line)
                if m is not None:
                    hh = m.group(1).decode('utf8')
                    ss = bytes.fromhex(hh)
                    if len(ss) > 0 and ss[0] == b'{' and ss[-1] == b'}':  # type: ignore[comparison-overlap]
                        # json-ish
                        # replace newlines just in case, otherwise it might mangle the sorting
                        ss = re.sub(rb'(\r\n|\r|\n)', b'<NEWLINE>', ss)
                        line = line[: m.start(1)] + ss + line[m.end(1) :]
                fo.write(line)
        # TODO maybe only do it in diff mode? not sure
        shutil.move(str(dump_file_nohex), str(dump_file))
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
