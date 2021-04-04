"""
Helpers for processing sqlite databases
"""
from contextlib import contextmanager
from pathlib import Path
import shutil
import sqlite3
from sqlite3 import Connection
from tempfile import TemporaryDirectory
from typing import Dict, Any, Iterator, Sequence, Optional, Callable, ContextManager, Type


from .common import CmpResult, Diff, Relation, logger, relations_to_instructions
from .processor import relations


from plumbum import local # type: ignore


def checked_no_wal(db: Path) -> Path:
    shm = db.parent / (db.name + '-shm')
    wal = db.parent / (db.name + '-wal')
    assert not shm.exists(), shm
    assert not wal.exists(), wal
    return db


def checked_db(db: Path) -> Path:
    # integrity check
    db = checked_no_wal(db)
    with sqlite3.connect(f'file:{db}?immutable=1', uri=True) as conn:
        list(conn.execute('pragma schema_version;'))
    db = checked_no_wal(db)
    return db


grep = local['grep']
sqlite_cmd = local['sqlite3']


# strip off 'creating' data in the database -- we're interested to spot whether it was deleted
SQLITE_GREP_FILTER = '> (INSERT INTO|CREATE TABLE) '
# FIXME need a test, i.e. with removing single line


def _dict2db(d: Dict, *, to: Path) -> Path:
    with sqlite3.connect(to) as conn:
        for table_name, rows in d.items():
            schema = rows[0]
            s = ', '.join(schema)
            qq = ', '.join('?' for _ in schema)
            conn.execute(f'CREATE TABLE {table_name} ({s})')
            conn.executemany(f'INSERT INTO {table_name} VALUES ({qq})', rows[1:])
    return to  # just for convenience


def test_db_relations(tmp_path: Path) -> None:
    # TODO this assumes they are already cleaned up?
    CR = CmpResult
    def ident(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = NoopSqliteNormaliser(path)
        return n.do_cleanup(path=path, wdir=wdir)


    # use single thread for test purposes
    func = lambda paths: relations(paths, cleanup=ident, grep_filter=SQLITE_GREP_FILTER, max_workers=1)


    d: Dict[str, Any] = dict()
    ### just one file
    db1 = _dict2db(d, to=tmp_path / '1.db')
    # just one file.. should be empty
    [] = func([db1])
    ###

    ### simple 'dominates' test
    d['t1'] = [
        ['col1', 'col2'],
        [1     , 2     ],
        [3     , 4     ],
    ]
    db2 = _dict2db(d, to=tmp_path / '2.db')

    [r11] = func([db1, db2])
    assert r11.before == db1
    assert r11.after  == db2
    assert r11.diff.cmp == CR.DOMINATES
    ###

    ### test error handling
    db3 = tmp_path / '3.db'
    db3.write_text('BAD')
    [r21, r22] = func([db1, db2, db3])
    assert r11 == r21
    assert r22.before == db2
    assert r22.after  == db3
    assert r22.diff.cmp == CR.ERROR
    ###


    ### test 'same' handling
    db4 = _dict2db(d, to=tmp_path / '4.db')
    db5 = _dict2db(d, to=tmp_path / '5.db')

    [r31, r32, r33, r34] = func([db1, db2, db3, db4, db5])
    assert r32 == r22
    assert r33.diff.cmp == CR.ERROR
    assert r34.diff.cmp == CR.SAME
    ###

    ### test when stuff was removed
    del d['t1'][-1]
    db6 = _dict2db(d, to=tmp_path / '6.db')
    [_, _, _, r44, r45] = func([db1, db2, db3, db4, db5, db6])
    assert r44 == r34
    assert r45.diff.cmp == CR.DIFFERENT
    ###


# TODO add some tests for my own dbs? e.g. stashed

class SqliteNormaliser:
    DELETE_DOMINATED = False

    @staticmethod
    def checked(db: Path) -> Connection:
        """common schema checks (for both cleanup/extract)"""
        db = checked_db(db)
        conn = sqlite3.connect(f'file:{db}?immutable=1', uri=True)
        return conn

    # TODO in principle we can get away with using only 'extract'?
    # 'cleanup' is just a sanity check? so you don't cleanup too much by accident?
    # guess it makes it easier to specify only one of them?
    # - by default, cleanup doesn't do anything
    # - by default, extract extracts everything
    # TODO needs to return if they are same or dominated?
    # for BM it's fine to delete dominated though..
    # except... FIXME need to keep filenames? this could be useful info...
    # need to decide where to log them...

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        db = path
        db = checked_db(db)
        with TemporaryDirectory() as tdir:
            td = Path(tdir)
            # FIXME should just use wdir?
            cleaned_db = td / (db.name + '-cleaned')
            shutil.copy(db, cleaned_db)
            with sqlite3.connect(cleaned_db) as conn:
                # prevent it from generating unnecessary wal files
                conn.execute('PRAGMA journal_mode=MEMORY;')
                self.cleanup(conn)
            cleaned_db = checked_db(cleaned_db)

            ### dump to text file
            ## prepare a fake path for dump, just to preserve original file paths at least to some extent
            assert cleaned_db.is_absolute(), cleaned_db
            dump_file = wdir / Path(*cleaned_db.parts[1:])  # cut off '/' and use relative path
            dump_file = dump_file.parent / f'{dump_file.name}-dump.sql'
            dump_file.parent.mkdir(parents=True, exist_ok=True)  # meh
            #
            dump_cmd = sqlite_cmd['-readonly', cleaned_db, '.dump']
            # can't filter it otherwise :( and can't drop it in filter
            filter_cmd = grep['-vE', '^INSERT INTO sqlite_sequence ']
            cmd = (dump_cmd | filter_cmd) > str(dump_file)
            cmd(retcode=(0, 1))
            ###
            yield dump_file


    def cleanup(self, c: Connection) -> None:
        # todo could have default implementation??
        raise NotImplementedError


class NoopSqliteNormaliser(SqliteNormaliser):
    def __init__(self, path: Path) -> None:
        pass

    def cleanup(self, c: Connection) -> None:
        pass


def sqlite_process(
        paths: Sequence[Path],
        *,
        Normaliser: Type[SqliteNormaliser],
        max_workers: Optional[int],
) -> None:
    from .common import Keep, Delete, Config

    def cleanup(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = Normaliser(p)  # type: ignore  # meh
        return n.do_cleanup(path, wdir=wdir)

    rels = list(relations(
        paths=paths,
        cleanup=cleanup,
        grep_filter=SQLITE_GREP_FILTER,
        max_workers=max_workers,
    ))
    cfg = Config(delete_dominated=Normaliser.DELETE_DOMINATED)
    instructions = relations_to_instructions(rels, config=cfg)
    for i in instructions:
        action = {Keep: 'keep', Delete: 'delete'}[type(i)]
        print(i.path, ': ', action)
