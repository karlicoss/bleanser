#!/usr/bin/env python3
from pathlib import Path
import shutil
from contextlib import contextmanager
import sqlite3
from sqlite3 import Connection
import subprocess
from typing import Sequence, List, Iterator, ContextManager
from tempfile import TemporaryDirectory


from bleanser.core.common import Relation, logger
from bleanser.core.sqlite import relations



def get_tables(c: Connection) -> List[str]:
    cur = c.execute('SELECT name FROM sqlite_master')
    names = [c[0] for c in cur]
    names.remove('sqlite_sequence') # hmm not sure about it
    return names


# todo rename?
class Normaliser:

    def __init__(self, db: Path) -> None:
        # self.db = db  # todo not sure if should know it... might result in some issues
        # todo not sure about this?.. also makes sense to run checked for cleanup/extract?
        with self.checked(db) as conn:
            self.tables = get_tables(conn)
            [info_table] = (x for x in self.tables if x.endswith('_info'))
            self.device, _ = info_table.split('_')


    @staticmethod
    def checked(db: Path) -> Connection:
        """common schema checks (for both cleanup/extract)"""
        # tmp dir for safety
        assert str(db).startswith('/tmp'), db
        shm = db.parent / (db.name + '-shm')
        wal = db.parent / (db.name + '-wal')
        assert not shm.exists(), shm
        assert not wal.exists(), wal

        conn = sqlite3.connect(db)
        return conn


    def cleanup(self, c: Connection) -> None:
        D = self.device

        ## get rid od downloadUnix -- it's changing after export and redundant info
        [[ut]] = list(c.execute(f'SELECT downloadUnix FROM {D}_info'))
        last_log = max(t for t in self.tables if t.endswith('log'))
        assert last_log == f'{D}_{ut}_log', last_log
        c.execute(f'UPDATE {D}_info set downloadUnix=-1')

    # TODO in principle we can get away with using only 'extract'?
    # 'cleanup' is just a sanity check? so you don't cleanup too much by accident?
    # guess it makes it easier to specify only one of them?
    # - by default, cleanup doesn't do anything
    # - by default, extract extracts everything
    # TODO needs to return if they are same or dominated?
    # for BM it's fine to delete dominated though..
    # except... need to keep filenames? this could be useful info...

    @contextmanager
    def do_cleanup(self, db: Path) -> Iterator[Path]:
        with TemporaryDirectory() as tdir:
            td = Path(tdir)
            output = td / (db.name + '-clean')
            shutil.copy(db, output)
            with sqlite3.connect(output) as conn:
                self.cleanup(conn)
            yield output


    def extract(self):
        tables = [
            # all deviceid_ts_log
            # all deviceid_ts_meta
            'settingsv2',
        ]


# todo uhh... again, useful to keep diff
def process(paths: Sequence[Path]) -> Iterator[Relation]:
    def cleanup(p: Path) -> ContextManager[Path]:
        n = Normaliser(p)
        return n.do_cleanup(p)
    return relations(
        paths=paths,
        cleanup=cleanup,
    )

def _run(*, path: Path) -> None:
    # TODO collect all sqlite mimes?
    paths = list(sorted(path.rglob('*.db')))
    for rel in process(paths):
        # TODO this would need to be iterative as well?
        print(rel)


def run(*, path: Path) -> None:
    with TemporaryDirectory() as tdir:
        td = Path(tdir) / 'dbs'
        shutil.copytree(path, td)
        _run(path=td)


import click
@click.command()
# @click.argument('file', type=Path, nargs=-1)
@click.argument('path', type=Path)
def main(path: Path) -> None:
    run(path=path)


if __name__ == '__main__':
    main()
