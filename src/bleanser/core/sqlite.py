"""
Helpers for processing sqlite databases
"""
from contextlib import contextmanager
from pathlib import Path
import sqlite3
from sqlite3 import Connection
from typing import Dict, Any, Iterator, Iterable, Sequence, Optional, Callable, ContextManager, List


from .common import logger, parametrize, Config
from .common import Keep, Delete, Group
from .processor import compute_groups, compute_instructions, BaseNormaliser


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
    conn.close()
    db = checked_no_wal(db)
    return db


grep = local['grep']
sqlite_cmd = local['sqlite3']


def _dict2db(d: Dict, *, to: Path) -> Path:
    with sqlite3.connect(to) as conn:
        for table_name, rows in d.items():
            schema = rows[0]
            s = ', '.join(schema)
            qq = ', '.join('?' for _ in schema)
            conn.execute(f'CREATE TABLE {table_name} ({s})')
            conn.executemany(f'INSERT INTO {table_name} VALUES ({qq})', rows[1:])
    conn.close()
    return to  # just for convenience


def test_sqlite(tmp_path: Path) -> None:
    # TODO this assumes they are already cleaned up?
    def ident(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = NoopSqliteNormaliser(path)
        return n.do_cleanup(path=path, wdir=wdir)

    config = Config(multiway=False)
    # use single thread for test purposes
    func = lambda paths: compute_groups(
        paths,
        cleanup=ident,
        diff_filter=NoopSqliteNormaliser.DIFF_FILTER, max_workers=1,
        config=config,
    )

    d: Dict[str, Any] = dict()
    ### just one file
    db1 = _dict2db(d, to=tmp_path / '1.db')
    [g11] = func([db1])
    assert g11.items  == [db1]
    assert g11.pivots == [db1]
    ###

    ### simple 'dominates' test
    d['t1'] = [
        ['col1', 'col2'],
        [1     , 2     ],
        [3     , 4     ],
    ]
    db2 = _dict2db(d, to=tmp_path / '2.db')

    [g21] = func([db1, db2])
    assert g21.items  == [db1, db2]
    assert g21.pivots == [db1, db2]
    ###

    ### test error handling
    db3 = tmp_path / '3.db'
    db3.write_text('BAD')

    # TODO wow it's really messy...
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
    # FIXME check error reason
    ###

    ### test 'same' handling
    db4 = _dict2db(d, to=tmp_path / '4.db')
    db5 = _dict2db(d, to=tmp_path / '5.db')
    db6 = _dict2db(d, to=tmp_path / '6.db')

    dbs = [db1, db2, db3, db4, db5, db6]
    [g41, g42, g43, g44] = func(dbs)
    assert g43 == g33
    assert g44.items  == [db4, db5, db6]
    assert g44.pivots == [db4, db6]
    ###

    instrs = compute_instructions(
        dbs,
        Normaliser=NoopSqliteNormaliser,
        max_workers=0
    )
    assert list(map(type, instrs)) == [
        Keep,   # 1
        Keep,   # 2
        Keep,   # 3
        Keep,   # 4, keep the boundary
        Delete, # 5
        Keep,   # 6, keep the boundary
    ]


    ### test when stuff was removed
    del d['t1'][-1]
    db7 = _dict2db(d, to=tmp_path / '7.db')
    dbs = [db1, db2, db3, db4, db5, db6, db7]
    [_, _, _, g54, g55, g56] = func(dbs)
    assert g54 == g44
    # TODO ugh. this is confusing... why it emits more pivots?
    assert g55.items  == [db6]
    assert g55.pivots == [db6]
    assert g56.items  == [db7]
    assert g56.pivots == [db7]
    ###

    instrs = compute_instructions(
        dbs,
        Normaliser=NoopSqliteNormaliser,
        max_workers=0
    )
    assert list(map(type, instrs)) == [
        Keep,   # 1
        Keep,   # 2
        Keep,   # 3
        Keep,   # 4, keep the boundary
        Delete, # 5
        Keep,   # 6, keep the boundary
        Keep,   # 7,
    ]


@parametrize('multiway', [False, True])
def test_sqlite_many(multiway: bool, tmp_path: Path) -> None:
    config = Config(multiway=multiway)
    N = 2000

    def ident(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = NoopSqliteNormaliser(path)
        return n.do_cleanup(path=path, wdir=wdir)

    paths = []
    d: Dict[str, Any] =  {}
    for i in range(N):
        if i % 10 == 0:
            # flush so sometimes it emits groups
            d = {'t': [('number',)]}
        d['t'].append((i,))
        p = _dict2db(d, to=tmp_path / f'{i:04}.db')
        paths.append(p)

    # shouldn't crash
    instrs = list(compute_instructions(
        paths,
        Normaliser=NoopSqliteNormaliser,
        max_workers=0
    ))


# TODO add some tests for my own dbs? e.g. stashed

class SqliteNormaliser(BaseNormaliser):
    # FIXME not sure if should be overridable
    # strip off 'creating' data in the database -- we're interested to spot whether it was deleted
    DIFF_FILTER = '> (INSERT INTO|CREATE TABLE) '
    # FIXME need a test, i.e. with removing single line

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
    # except... need to keep filenames? this could be useful info...
    # need to decide where to log them...

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        db = path
        db = checked_db(db)

        # ugh. in principle could use :memory: database here...
        # but then dumping it via iterdump() takes much more time then sqlite3 .dump command..
        path = path.absolute().resolve()
        cleaned_db = wdir / Path(*path.parts[1:]) / (db.name + '-cleaned')
        cleaned_db.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy(db, cleaned_db)
        with sqlite3.connect(cleaned_db) as conn:
            # prevent it from generating unnecessary wal files
            conn.execute('PRAGMA journal_mode=MEMORY;')
            # we don't care about constraints in cleanup stage
            conn.execute('PRAGMA foreign_keys = OFF;')
            # FIXME
            # need to drop all constraints, foreign keys and uniqueness stuff
            # can't do this in sqlite though... only via temporary table?

            # cleanup might take a bit of time, especially with UPDATE statemens
            # but probably unavoidable?
            self.cleanup(conn)
        conn.close()
        cleaned_db = checked_db(cleaned_db)

        ### dump to text file
        ## prepare a fake path for dump, just to preserve original file paths at least to some extent
        assert cleaned_db.is_absolute(), cleaned_db
        dump_file = wdir / Path(*cleaned_db.parts[1:])  # cut off '/' and use relative path
        dump_file = dump_file.parent / f'{dump_file.name}-dump.sql'
        dump_file.parent.mkdir(parents=True, exist_ok=True)  # meh
        ##
        # dumping also takes a bit of time for big databases...
        dump_cmd = sqlite_cmd['-readonly', cleaned_db, '.dump']
        # can't filter it otherwise :( and can't drop it in filter
        filter_cmd = grep['-vE', '^INSERT INTO sqlite_sequence ']
        cmd = (dump_cmd | filter_cmd) > str(dump_file)
        cmd(retcode=(0, 1))

        cleaned_db.unlink()
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


class Tool:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    # FIXME quoting
    def drop(self, table: str) -> None:
        self.connection.execute(f'DROP TABLE IF EXISTS {table}')

    def drop_view(self, view: str) -> None:
        self.connection.execute(f'DROP VIEW IF EXISTS {view}')

    def drop_index(self, index: str) -> None:
        self.connection.execute(f'DROP INDEX IF EXISTS {index}')

    def update(self, table: str, **kwargs) -> None:
        kws = ', '.join(f'{k}=?' for k, v in kwargs.items())
        self.connection.execute(f'UPDATE {table} set {kws}', list(kwargs.values()))

    def drop_cols(self, *, table: str, cols: Sequence[str]) -> None:
        # for the purposes of comparison this is same as dropping
        # for update need to filter nonexisting cols
        #
        cur = self.connection.execute(f'PRAGMA table_info({table})')
        existing = [r[1] for r in cur]
        # todo warn maybe if dropped columns?
        cols = [c for c in cols if c in existing]
        if len(cols) == 0:
            return
        self.update(table, **{col: '' for col in cols})
        # TODO crap. https://stackoverflow.com/a/66399224/706389
        # alter table is since march 2021... so won't be in sqlite for a while
        # for col in cols:
        #     c.execute(f'ALTER TABLE {table} DROP COLUMN {col}')
