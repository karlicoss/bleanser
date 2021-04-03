"""
Helpers for processing sqlite databases
"""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
from dataclasses import dataclass
from pathlib import Path
import re
import sqlite3
from sqlite3 import Connection
import subprocess
from subprocess import DEVNULL
from tempfile import TemporaryDirectory
from typing import Dict, Any, Iterator, Sequence, Optional, Tuple, Optional, Union, Callable, ContextManager

from .common import CmpResult, Diff, Relation, logger

import more_itertools
from plumbum import local # type: ignore


def check_db(p: Path) -> None:
    # integrity check
    subprocess.check_call(['sqlite3', '-readonly', p, 'pragma schema_version;'], stdout=DEVNULL)


diff = local['diff']
grep = local['grep']


Input = Path
Cleaned = Path
Cleaner = Callable[[Input], ContextManager[Cleaned]]


def relations(
        paths: Sequence[Path],
        *,
        cleanup: Cleaner,
        max_workers: Optional[int]=None,
) -> Iterator[Relation]:
    assert max_workers != 0  # FIXME handle it in serial mode then?
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        workers = getattr(pool, '_max_workers')
        morkers = min(workers, len(paths))  # no point in using too many workers
        logger.info('using %d workers', workers)

        chunks = []
        futures = []
        for paths_chunk in more_itertools.divide(workers, paths):
            pp = list(paths_chunk)
            if len(pp) == 0:
                continue
            chunks.append(pp)
            futures.append(pool.submit(
                # force iterator, otherwise it'll still be basically serial
                lambda *args, **kwargs: list(_relations_serial(*args, **kwargs)),
                paths=pp,
                cleanup=cleanup,
            ))
        emitted = 0
        last: Optional[Path] = None
        for chunk, f in zip(chunks, futures):
            if last is not None:
                # yield fake relation just to fill the gap between chunks...
                # TODO kinda annying since it won't be idempotent...
                emitted += 1
                yield Relation(before=last, after=chunk[0], diff=Diff(cmp=CmpResult.DIFFERENT, diff=b''))
            last = chunk[0]
            rit = f.result()
            for r in rit:
                emitted += 1
                yield r
                last = r.after
        assert emitted == len(paths) - 1, (paths, emitted)


# todo these are already normalized paths?
# although then harder to handle exceptions... ugh
def _relations_serial(
        paths: Sequence[Path],
        *,
        cleanup: Cleaner,
) -> Iterator[Relation]:
    assert len(paths) > 0
    # fast track.. so we don't compute dumps
    if len(paths) == 1:
        return []

    XX = Tuple[Input, Union[Exception, Cleaned]]
    XXX = Tuple[XX, XX]

    def outputs() -> Iterator[XXX]:
        with ExitStack() as stack:
            last: Optional[XX] = None
            for cp in paths:
                # TODO need to copy to tmp first??
                assert str(cp).startswith('/tmp'), cp

                td = stack.enter_context(TemporaryDirectory())
                tdir = Path(td)

                dump_file = tdir / 'dump.sql'
                next_: XX
                try:
                    check_db(cp)
                    cleaned_db = stack.enter_context(cleanup(cp))
                    check_db(cleaned_db)
                except Exception as e:
                    logger.exception(e)
                    next_ = (cp, e)
                else:
                    with dump_file.open('w') as fo:
                        subprocess.run(['sqlite3', '-readonly', cleaned_db, '.dump'], check=True, stdout=fo)
                    next_ = (cp, dump_file)

                if last is not None:
                    yield (last, next_)
                last = next_

    for [(p1, dump1), (p2, dump2)] in outputs():
        logger.info("cleanup: %s vs %s", p1, p2)
        logger.debug("%s: %s", p1, dump1)
        logger.debug("%s: %s", p2, dump2)
        # TODO could also use sort + comm? not sure...
        # sorting might be a good idea actually... would work better with triples?

        if isinstance(dump1, Exception) or isinstance(dump2, Exception):
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.ERROR))
            continue

        # just for mypy...
        assert isinstance(dump1, Path), dump1
        assert isinstance(dump2, Path), dump2

        # print(diff[dump1, dump2](retcode=(0, 1)))  # for debug

        # strip off 'creating' data in the database -- we're interested to spot whether it was deleted
        cmd = diff[dump1, dump2]  | grep['-vE', '> (INSERT INTO|CREATE TABLE) ']
        res = cmd(retcode=(0, 1))
        if len(res) > 10000:  # fast track to fail
            # TODO Meh
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DIFFERENT))
            continue
        rem = res.splitlines()
        # clean up diff crap like
        # 756587a756588,762590
        rem = [l for l in rem if not re.fullmatch(r'\d+a\d+(,\d+)?', l)]
        if len(rem) == 0:
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DOMINATES))
        else:
            yield Relation(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DIFFERENT))


def _dict2db(d: Dict, *, to: Path) -> Path:
    with sqlite3.connect(to) as conn:
        for table_name, rows in d.items():
            schema = rows[0]
            s = ', '.join(schema)
            qq = ', '.join('?' for _ in schema)
            conn.execute(f'CREATE TABLE {table_name} ({s})')
            conn.executemany(f'INSERT INTO {table_name} VALUES ({qq})', rows[1:])
    return to  # just for convenience


def test_relations(tmp_path: Path) -> None:
    # TODO this assumes they are already cleaned up?
    CR = CmpResult
    @contextmanager
    def ident(p: Path) -> Iterator[Path]:
        yield p


    # set max_workers to 1,
    func = lambda paths: relations(paths, cleanup=ident, max_workers=1)


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
    assert r34.diff.cmp == CR.DOMINATES  # FIXME should be SAME later...
    ###

    ### test when stuff was removed
    del d['t1'][-1]
    db6 = _dict2db(d, to=tmp_path / '6.db')
    [_, _, _, r44, r45] = func([db1, db2, db3, db4, db5, db6])
    assert r44 == r34
    assert r45.diff.cmp == CR.DIFFERENT
    ###


# TODO add some tests for my own dbs? e.g. stashed
