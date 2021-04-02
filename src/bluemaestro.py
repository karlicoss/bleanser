#!/usr/bin/env python3
from pathlib import Path
import shutil
from contextlib import contextmanager, ExitStack
import re
import sqlite3
from sqlite3 import Connection
import subprocess
from subprocess import PIPE
from typing import Sequence, List, Iterator
from tempfile import TemporaryDirectory

from more_itertools import windowed


def get_tables(c: Connection) -> List[str]:
    cur = c.execute('SELECT name FROM sqlite_master')
    names = [c[0] for c in cur]
    names.remove('sqlite_sequence') # hmm not sure about it
    return names


# todo rename?
class Normaliser:

    def __init__(self, db: Path) -> None:
        assert str(db).startswith('/tmp'), db
        self.db = db
        # FIXME assert no journals/wals?

        with sqlite3.connect(db) as conn:
            self.tables = get_tables(conn)
            [info_table] = (x for x in self.tables if x.endswith('_info'))
            self.device, _ = info_table.split('_')


    def _cleanup(self, c: Connection):
        D = self.device

        ## get rid od downloadUnix -- it's changing after export and redundant info
        [[ut]] = list(c.execute(f'SELECT downloadUnix FROM {D}_info'))
        last_log = max(t for t in self.tables if t.endswith('log'))
        assert last_log == f'{D}_{ut}_log', last_log
        c.execute(f'UPDATE {D}_info set downloadUnix=-1')


    @contextmanager
    def cleanup(self) -> Iterator[Path]:
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
    paths = list(sorted(path.rglob('*.db')))

    from typing import Tuple, Iterator
    def outputs() -> Iterator: # [Tuple[Path, Path]]:
        with ExitStack() as stack:
            lp = None
            lres = None
            for cp in paths:
                n = Normaliser(cp)
                cres = stack.enter_context(n.cleanup())
                if lp is not None and lres is not None:
                    yield ((lp, lres), (cp, cres))
                lp = cp
                lres = cres



    for [(p1, dump1), (p2, dump2)] in outputs():
        print(f"cleanup: {p1} vs {p2}")
        # TODO could also use sort + comm? not sure...
        cmd = diff[dump1, dump2]  | grep['-vE', '> (INSERT INTO|CREATE TABLE) ']
        res = cmd(retcode=(0, 1))
        if len(res) > 10000:  # fast track to fail
            # TODO Meh
            print(res)
            print("FAILURE!!!")
        rem = res.splitlines()
        # clean up diff crap like
        # 756587a756588,762590
        rem = [l for l in rem if not re.fullmatch(r'\d+a\d+,\d+', l)]
        if len(rem) == 0:
            print("ALL GOOD!")
        else:
            # TODO not sure if really should print the diff...
            print(res)
            print("FAILURE!!!")
        print('-------------')


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
