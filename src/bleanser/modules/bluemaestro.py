#!/usr/bin/env python3
from pathlib import Path
import shutil
from contextlib import contextmanager, ExitStack
import re
import sqlite3
from sqlite3 import Connection
import subprocess
from subprocess import PIPE
from typing import Sequence, List, Iterator, Tuple, Optional, Union
from tempfile import TemporaryDirectory

from bleanser.core.common import CmpResult, Diff, Relation

from kython.klogging2 import LazyLogger


logger = LazyLogger(__name__, level='debug')


def get_tables(c: Connection) -> List[str]:
    cur = c.execute('SELECT name FROM sqlite_master')
    names = [c[0] for c in cur]
    names.remove('sqlite_sequence') # hmm not sure about it
    return names


# todo rename?
class Normaliser:

    def __init__(self, db: Path) -> None:
        self.db = db

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


    def _cleanup(self, c: Connection):
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
    def cleanup(self) -> Iterator[Path]:
        # todo maybe shouldn't be ctx manager? then would be easier to pass in a separate process
        with TemporaryDirectory() as tdir:
            td = Path(tdir)
            to = td / self.db.name
            shutil.copy(self.db, to)
            with sqlite3.connect(to) as conn:
                self._cleanup(conn)
            # todo yield outstream? not sure..
            outf = td / 'dump'
            with outf.open('w') as fo:
                res = subprocess.run(['sqlite3', to, '.dump'], check=True, stdout=fo)
            yield outf


    def extract(self):
        tables = [
            # all deviceid_ts_log
            # all deviceid_ts_meta
            'settingsv2',
        ]


from plumbum import local # type: ignore
diff = local['diff']
grep = local['grep']


def _run(*, path: Path) -> None:
    # TODO collect all sqlite mimes?
    paths = list(sorted(path.rglob('*.db')))
    for a, b, res in process(paths):
        # TODO this would need to be iterative as well?
        print(a, b, res)


Input = Path
Cleaned = Path
XX = Tuple[Input, Union[Exception, Cleaned]]

XXX = Tuple[XX, XX]

# todo uhh... again, useful to keep diff
def process(paths: Sequence[Path]) -> Iterator[Relation]:
    def outputs() -> Iterator[XXX]:
        with ExitStack() as stack:
            last: Optional[XX] = None
            for cp in paths:
                n = Normaliser(cp)
                next_: XX
                try:
                    cres = stack.enter_context(n.cleanup())
                except Exception as e:
                    next_ = (cp, e)
                else:
                    next_ = (cp, cres)

                if last is not None:
                    yield (last, next_)
                last = next_


    # TODO for multiprocessing, not sure what's the best way to do it...
    for [(p1, dump1), (p2, dump2)] in outputs():
        logger.info("cleanup: %s vs %s", p1, p2)
        logger.debug("%s: %s", p1, dump1)
        logger.debug("%s: %s", p2, dump2)
        # TODO could also use sort + comm? not sure...
        # sorting might be a good idea actually... would work better with triples?
        # FIXME cover this with tests

        if isinstance(dump1, Exception) or isinstance(dump2, Exception):
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.ERROR))
            continue

        # just for mypy...
        assert isinstance(dump1, Path), dump1
        assert isinstance(dump1, Path), dump2

        cmd = diff[dump1, dump2]  | grep['-vE', '> (INSERT INTO|CREATE TABLE) ']
        res = cmd(retcode=(0, 1))
        if len(res) > 10000:  # fast track to fail
            # TODO Meh
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DIFFERENT))
            continue
        rem = res.splitlines()
        # clean up diff crap like
        # 756587a756588,762590
        rem = [l for l in rem if not re.fullmatch(r'\d+a\d+,\d+', l)]
        if len(rem) == 0:
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DOMINATES))
        else:
            # TODO not sure if really should print the diff...
            # print(res)
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DIFFERENT))


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
