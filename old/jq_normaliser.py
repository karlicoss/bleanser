#!/usr/bin/env python3
from argparse import ArgumentParser
import json
import logging
from pathlib import Path
from subprocess import check_output, check_call, PIPE, run, Popen
import sys
from typing import Optional, List, Iterator, Iterable, Tuple, Optional, Union, NamedTuple, Sequence
from tempfile import TemporaryDirectory
# make sure doesn't contain '<'

from .common import CmpResult, Diff, Relation


from kython import numbers

from kython.kjq import Json, JsonFilter, del_all_kjson
from kython.klogging2 import LazyLogger

from multiprocessing import Pool


JqFilter = str

Fields = Sequence[str]

class Filter2(NamedTuple):
    jq: JqFilter
    # ugh!
    extra_del_all: Optional[Fields] = None


Filter = Union[JqFilter, Filter2]



def _jq(path: Path, filt: Filter, fo):
    # TODO from kython import kompress
    # # TODO shit.  why kompress is unhappy??
    # with kompress.open(path, 'rb') as fi:
    #     # # import ipdb; ipdb.set_trace()  
    #     # sys.stdout.buffer.write(fi.read())
    #     # raise RuntimeError
    #     p = Popen(cmd, stdout=fo, stdin=fi)
    #     _, _ = p.communicate()
    #     assert p.returncode == 0
    with TemporaryDirectory() as td:
        tdir = Path(td)
        if path.suffix.endswith('.xz'):
            upath = tdir.joinpath('unpacked')
            out = check_output(['aunpack', '-c', str(path)])
            upath.write_bytes(out)
            path = upath
        if isinstance(filt, Filter2):
            extra = filt.extra_del_all
            if extra is not None:
                jstr = path.read_text()
                j = json.loads(jstr)
                j = del_all_kjson(*extra)(j)
                path.write_text(json.dumps(j))
            jq_cmd = ['jq', filt.jq]
        else:
            jq_cmd = ['jq', filt]
        return check_call(jq_cmd + [str(path)], stdout=fo)


def jq(path: Path, filt: Filter, output: Path):
    with output.open('wb') as fo:
        _jq(path=path, filt=filt, fo=fo)

Result = List[Path]


R = CmpResult


class XX(NamedTuple):
    path: Path
    rel_next: Optional[Relation]


class JqNormaliser:
    def __init__(
            self,
            logger_tag='normaliser',
            delete_dominated=False,
            keep_both=True,
    ) -> None:
        self.logger = LazyLogger(logger_tag, level='debug')
        self.delete_dominated = delete_dominated
        self.keep_both = keep_both
        self.errors: List[str] = []
        self.print_diff = False

    def main(self, glob: str='*.json'):
        p = ArgumentParser()
        p.add_argument('before', type=Path, nargs='?')
        p.add_argument('after', type=Path, nargs='?')
        p.add_argument('--dry', action='store_true')
        p.add_argument('--all', type=Path, default=None)
        p.add_argument('--print-diff', action='store_true')
        p.add_argument('--extract', '-e', action='store_true')
        p.add_argument('--redo', action='store_false', dest='continue')
        p.add_argument('--cores', type=int, required=False)
        args = p.parse_args()

        self.print_diff = args.print_diff # meh

        normalise_file = None

        if args.all is not None:
            last_processed = None
            start_from = None
            if getattr(args, 'continue'):
                processed = list(sorted(args.all.glob('*.bleanser')))
                if len(processed) > 0:
                    last_processed = processed[-1]
                    start_from = last_processed.parent / last_processed.name[:-len('.bleanser')]
            files = list(sorted(args.all.glob(glob)))
            if start_from is not None:
                assert start_from is not None
                self.logger.info('processed up to: %s. continuing', last_processed)
                files = [f for f in files if f >= start_from]
        else:
            assert args.before is not None

            if args.after is not None:
                files = [args.before, args.after]
            else:
                normalise_file = args.before

        if normalise_file is not None:
            filt = self.extract() if args.extract else self.cleanup()
            _jq(path=normalise_file, filt=filt, fo=sys.stdout)

        else:
            self.do(files=files, dry_run=args.dry, cores=args.cores)

            if len(self.errors) > 0:
                for e in self.errors:
                    self.logger.error(e)
                sys.exit(1)


    def extract(self) -> Filter:
        return NotImplemented

    def cleanup(self) -> Filter:
        raise NotImplementedError

    # TODO need to check that it agrees with extract about changes
    def diff_with(self, before: Path, after: Path, cmd: Filter, tdir: Path, *, cores=2) -> Diff:
        norm_before = tdir.joinpath('before')
        norm_after = tdir.joinpath('after')

        self.logger.debug('diffing: %s %s', before, after)

        with Pool(cores) as p:
            # eh, weird, couldn't figureo ut how to use p.map so it unpacks tuples
            r1 = p.apply_async(jq, (before, cmd, norm_before))
            r2 = p.apply_async(jq, (after , cmd, norm_after))
            r1.get()
            r2.get()

        dres = run([
            'diff', str(norm_before), str(norm_after)
        ], stdout=PIPE)
        assert dres.returncode <= 1
        diff = dres.stdout
        diff_lines = diff.decode('utf8').splitlines()
        removed: List[str] = []
        for l in diff_lines:
            if l.startswith('<'):
                removed.append(l)

        if len(removed) == 0:
            if dres.returncode == 0:
                return Diff(CmpResult.SAME, diff)
            else:
                return Diff(CmpResult.DOMINATES, diff)
        else:
            return Diff(CmpResult.DIFFERENT, diff)

    def _compare(self, before: Path, after: Path, cores: int, *, tdir: Path) -> Diff:
        diff_cleanup = self.diff_with(before, after, self.cleanup(), tdir=tdir, cores=cores)
        if self.print_diff:
            self.logger.info('cleanup diff:')
            sys.stderr.write(diff_cleanup.diff.decode('utf8'))
        extr = self.extract()
        if extr is not NotImplemented:
            diff_extract = self.diff_with(before, after, extr, tdir=tdir, cores=cores)
            if self.print_diff:
                self.logger.info('extract diff:')
                sys.stderr.write(diff_extract.diff.decode('utf8'))

            if diff_cleanup.cmp != diff_extract.cmp:
                err = f'while comparing {before} {after} : cleanup gives {diff_cleanup.cmp} whereas extraction gives {diff_extract.cmp}'
                self.logger.error(err)
                self.errors.append(err)
        return diff_cleanup


    def compare(self, *args, **kwargs) -> Diff:
        with TemporaryDirectory() as tdir:
            return self._compare(*args, **kwargs, tdir=Path(tdir)) # type: ignore

    def _iter_groups(self, relations: Iterable[Relation]):
        from typing import Any
        group: List[XX] = []

        def dump_group():
            if len(group) == 0:
                return []
            res = [g for g in group]
            group.clear()
            return [res]

        def group_add(path, rel):
            group.append(XX(path=path, rel_next=rel))

        last = None
        for i, rel in zip(numbers(), relations):
            if i != 0:
                assert last == rel.before
            last = rel.after

            res = rel.diff.cmp

            if res == CmpResult.DOMINATES:
                res = CmpResult.SAME if self.delete_dominated else CmpResult.DIFFERENT

            if res == CmpResult.DIFFERENT:
                group_add(rel.before, None)
                yield from dump_group()
            else:
                assert res == CmpResult.SAME
                group_add(rel.before, rel)
        group_add(last, None)
        yield from dump_group()

    def _iter_deleted(self, relations: Iterable[Relation]) -> Iterator[XX]:
        groups = self._iter_groups(relations)
        for g in groups:
            if len(g) <= 1:
                continue
            delete_start = 1 if self.keep_both else 0
            yield from g[delete_start: -1]

    def _iter_relations(self, files, *, cores=None) -> Iterator[Relation]:
        from concurrent.futures import ProcessPoolExecutor

        with ProcessPoolExecutor(cores) as pool:
            it = list(zip(range(len(files)), files, files[1:]))
            futures = []
            for i, before, after in it:
                CORES = 1
                futures.append(pool.submit(self.compare, before, after, cores=CORES)) # TODO in multicore mode diff should only use one core...

            for (i, before, after), f in zip(it, futures):
                self.logger.info('comparing %d: %s   %s', i, before, after)
                res, diff = f.result()
                self.logger.info('result: %s', res)
                yield Relation(
                    before=before,
                    diff=Diff(cmp=res, diff=diff),
                    after=after,
                )

    def do(self, files, dry_run=True, cores=None) -> None:
        assert len(files) > 0
        self.logger.debug('running: dry: %s, cores: %s, files: %s', dry_run, cores, files)
        def rm(pp: XX):
            bfile = pp.path.parent.joinpath(pp.path.name + '.bleanser')
            rel = pp.rel_next
            assert rel is not None

            with bfile.open('wb') as fo:
                fline = f'comparing {rel.before} vs {rel.after}: {rel.diff.cmp}\n'
                fo.write(fline.encode('utf8'))
                fo.write(rel.diff.diff)

            if dry_run:
                self.logger.warning('dry run! would remove %s', pp.path)
            else:
                self.logger.warning('removing: %s', pp.path)
                pp.path.unlink()

        # TODO would be nice to use multiprocessing here... 
        relations = self._iter_relations(files=files, cores=cores)
        for d in self._iter_deleted(relations):
            rm(d)


def asrel(files, results) -> Iterator[Relation]:
    assert len(files) == len(results) + 1
    for b, res, a in zip(files, results, files[1:]):
        yield Relation(before=b, diff=Diff(res, b''), after=a)

def test0():
    P = Path
    nn = JqNormaliser(
        delete_dominated=True,
    )
    assert [[x.path for x in n] for n in nn._iter_groups(asrel(
        files=[
            P('a'),
            P('b'),
        ],
        results=[
            R.SAME,
        ],
    ))] == [
        [P('a'), P('b')],
    ]


def test1():
    P = Path
    # TODO kython this? it's quite common..
    nn = JqNormaliser(
        delete_dominated=True,
    )
    assert [[x.path for x in n] for n in nn._iter_groups(asrel(
        files=[
            P('a'),
            P('b'),
            P('c'),
            P('d'),
            P('e'),
            P('f'),
            P('g'),
            P('h'),
        ],
        results=[
            R.SAME, # ab
            R.DOMINATES, # bc
            R.DIFFERENT, # cd
            R.SAME, # de
            R.DIFFERENT, # ef
            R.SAME, # fg
            R.SAME, # gh
        ]
    ))]  == [
        [P('a'), P('b'), P('c')],
        [P('d'), P('e')],
        [P('f'), P('g'), P('h')],
    ]

def test2():
    P = Path
    files = [
        P('a'),
        P('b'),
        P('c'),
        P('d'),
        P('e'),
        P('f'),
        P('g'),
        P('h'),
    ]
    results = [
        R.DIFFERENT,
        R.DOMINATES,
        R.SAME,
        R.SAME,
        R.SAME,
        R.DIFFERENT,
        R.DOMINATES,
    ]
    nn = JqNormaliser(
        delete_dominated=False,
        keep_both=True,
    )
    assert [x.path for x in nn._iter_deleted(asrel(
        files=files,
        results=results,
    ))] == [P('d'), P('e')]

    nn2 = JqNormaliser(
        delete_dominated=True,
        keep_both=False,
    )
    assert [x.path for x in nn2._iter_deleted(asrel(
        files=files,
        results=results,
    ))] == [P('b'), P('c'), P('d'), P('e'), P('g')]


from kython.kjq import pipe, jdel, jq_del_all


