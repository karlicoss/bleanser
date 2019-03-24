#!/usr/bin/env python3
import sys
from argparse import ArgumentParser
import logging
from pathlib import Path
from subprocess import check_output, check_call, PIPE, run
from typing import Optional, List, Iterator, Iterable, Tuple, Optional
from tempfile import TemporaryDirectory
# make sure doesn't conain '<'

from kython import numbers
from kython.klogging import setup_logzero

# TODO ok, it should only start with '>' I guess?

Filter = str

def jq(path: Path, filt: Filter, output: Path):
    with output.open('wb') as fo:
        check_call(['jq', filt, str(path)], stdout=fo)

Result = List[Path]

from enum import Enum, auto

class CmpResult(Enum):
    DIFFERENT = 'different'
    SAME = 'same'
    DOMINATES = 'dominates'
R = CmpResult

from typing import NamedTuple


class Diff(NamedTuple):
    cmp: CmpResult
    diff: bytes

class Relation(NamedTuple):
    before: Path
    diff: Diff
    after: Path


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
        self.logger = logging.getLogger()
        self.delete_dominated = delete_dominated
        self.keep_both = keep_both

    def main(self, all_files: List[Path]):
        setup_logzero(self.logger, level=logging.DEBUG)

        p = ArgumentParser()
        p.add_argument('before', type=Path, nargs='?')
        p.add_argument('after', type=Path, nargs='?')
        p.add_argument('--dry', action='store_true')
        p.add_argument('--all', action='store_true')
        p.add_argument('--print-diff', action='store_true')
        args = p.parse_args()
        if args.all:
            files = all_files
        else:
            assert args.before is not None
            assert args.after is not None
            files = [args.before, args.after]

        self.do(files=files, dry_run=args.dry, print_diff=args.print_diff)

    def extract(self) -> Filter:
        raise NotImplementedError

    def cleanup(self) -> Filter:
        raise NotImplementedError

    def _compare(self, before: Path, after: Path, tdir: Path) -> Diff:
        cmd = self.extract()
        norm_before = tdir.joinpath('before')
        norm_after = tdir.joinpath('after')

        jq(path=before, filt=cmd, output=norm_before)
        jq(path=after, filt=cmd, output=norm_after)

        # TODO hot to make it interactive? just output the command to compute diff?
        # TODO keep tmp dir??
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

    def _iter_relations(self, files, print_diff=False) -> Iterator[Relation]:
        for i, before, after in zip(range(len(files)), files, files[1:]):
            self.logger.info('comparing %d: %s   %s', i, before, after)
            res, diff = self.compare(before, after)
            self.logger.info('result: %s', res)
            if print_diff:
                sys.stdout.write(diff.decode('utf8'))
            yield Relation(
                before=before,
                diff=Diff(cmp=res, diff=diff),
                after=after,
            )

    def do(self, files, dry_run=True, print_diff=False) -> None:
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

        relations = self._iter_relations(files=files, print_diff=print_diff)
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


