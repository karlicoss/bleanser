#!/usr/bin/env python3
from argparse import ArgumentParser
import logging
from pathlib import Path
from subprocess import check_output, check_call, PIPE, run
from typing import Optional, List
from tempfile import TemporaryDirectory
# make sure doesn't conain '<'

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


class Normaliser:
    def __init__(
            self,
            logger_tag='normaliser',
            delete_dominated=False,
            keep_both=True,
    ) -> None:
        self.logger = logging.getLogger()
        self.delete_dominated = delete_dominated
        self.keep_both = keep_both

    def main(self):
        setup_logzero(self.logger, level=logging.DEBUG)

    def extract(self) -> Filter:
        raise NotImplementedError

    def cleanup(self) -> Filter:
        raise NotImplementedError

    def _compare(self, before: Path, after: Path, tdir: Path) -> CmpResult:
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

        diff_lines = dres.stdout.decode('utf8').splitlines()
        removed: List[str] = []
        for l in diff_lines:
            if l.startswith('<'):
                removed.append(l)

        if len(removed) == 0:
            if dres.returncode == 0:
                return CmpResult.SAME
            else:
                return CmpResult.DOMINATES
        else:
            return CmpResult.DIFFERENT

    def compare(self, *args, **kwargs) -> CmpResult:
        with TemporaryDirectory() as tdir:
            return self._compare(*args, **kwargs, tdir=Path(tdir)) # type: ignore

    def _get_groups(self, files: List[Path], results: List[CmpResult]):
        assert len(files) == len(results) + 1
        groups = []
        group: List[Path] = []
        def dump_group():
            assert len(group) > 0
            groups.append([g for g in group])
            group.clear()

        group.append(files[0])
        for i, before, res, after in zip(range(len(files)), files, results, files[1:]):
            if res == CmpResult.DOMINATES:
                res = CmpResult.SAME if self.delete_dominated else CmpResult.DIFFERENT
            if res == CmpResult.DIFFERENT:
                dump_group()
                group.append(after)
            else:
                assert res == CmpResult.SAME
                group.append(after)
        dump_group()
        return groups

    def _get_deleted(self, files: List[Path], results: List[CmpResult]) -> List[Path]:
        groups = self._get_groups(files, results)
        deleted: List[Path] = []
        for g in groups:
            if len(g) <= 1:
                continue
            delete_start = 1 if self.keep_both else 0
            deleted.extend(g[delete_start: -1])
        return deleted

    def do(self, files, dry_run=True) -> None:
        def rm(pp: Path):
            if dry_run:
                self.logger.warning('dry run! would remove %s', pp)
            else:
                raise RuntimeError

        results = []
        for i, before, after in zip(range(len(files)), files, files[1:]):
            self.logger.info('comparing %d: %s   %s', i, before, after)
            res = self.compare(before, after)
            self.logger.info('result: %s', res)
            results.append(res)

        # always try to keep latest backup
        # in dominated mode, just treat dominated as same

        # a DIFF b SAME c DOM d SAME e DIFF f SAME g SAME h
        # TODO extra mode for reddit for instance:
        # one mode would keep a, e, h (basically last in every group)
        # another  would keep a, b, e, f and h

def test():
    P = Path
    # TODO kython this? it's quite common..
    nn = Normaliser(
        delete_dominated=True,
    )
    assert nn._get_groups(
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
    )  == [
        [P('a'), P('b'), P('c')],
        [P('d'), P('e')],
        [P('f'), P('g'), P('h')],
    ]

def test2():
    P = Path
    nn = Normaliser(
        delete_dominated=False,
        keep_both=True,
    )
    assert nn._get_deleted(
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
            R.DIFFERENT,
            R.DOMINATES,
            R.SAME,
            R.SAME,
            R.SAME,
            R.DIFFERENT,
            R.DOMINATES,
        ]
    ) == [P('d'), P('e')]




ID_FILTER = '.'

class LastfmNormaliser(Normaliser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, logger_tag='lastfm-normaliser', delete_dominated=True)

    def extract(self) -> Filter:
        return 'sort_by(.date) | map(map_values(ascii_downcase))'

    def cleanup(self) -> Filter:
        return ID_FILTER


# TODO FIXME make sure to store .bleanser file with diff? or don't bother?


def main():
    bdir = Path('lastfm')

    norm = LastfmNormaliser()
    norm.main()
    p = ArgumentParser()
    p.add_argument('--dry', action='store_true')
    p.add_argument('before', nargs='?')
    p.add_argument('after', nargs='?')
    p.add_argument('--all', action='store_true')
    args = p.parse_args()
    if args.all:
        backups = list(sorted(bdir.glob('*.json')))
    else:
        assert args.before is not None
        assert args.after is not None
        backups = [args.before, args.after]

    norm.do(backups, dry_run=args.dry)

if __name__ == '__main__':
    main()
