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


class Normaliser:
    def __init__(self, logger_tag='normaliser') -> None:
        self.logger = logging.getLogger()
        # TODO main function??

    def main(self):
        setup_logzero(self.logger, level=logging.DEBUG)

    def extract(self) -> Filter:
        raise NotImplementedError

    def cleanup(self) -> Filter:
        raise NotImplementedError

    def _compare(self, before: Path, after: Path, tdir: Path):
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
            self.logger.info("no lines removed!")
        else:
            for l in diff_lines:
                print(l)

    def compare(self, *args, **kwargs) -> None:
        with TemporaryDirectory() as tdir:
            self._compare(*args, **kwargs, tdir=Path(tdir))
        # TODO diff??

    def do(self, files) -> None:
        for i, before, after in zip(range(len(files)), files, files[1:]):
            self.logger.info('comparing %d: %s   %s', i, before, after)
            self.compare(before, after)



ID_FILTER = '.'

class LastfmNormaliser(Normaliser):
    def extract(self) -> Filter:
        # TODO sort by date?
        # TODO shit looks like names are changing often...
        return 'sort_by(.date) | map(map_values(ascii_downcase))'
# with_entries( .key |= ascii_downcase )
    # TODO ignore case?

    def cleanup(self) -> Filter:
        return ID_FILTER


# TODO FIXME make sure to store .bleanser file with diff? or don't bother?


def main():
    bdir = Path('lastfm')

    norm = LastfmNormaliser()
    norm.main()
    p = ArgumentParser()
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

    norm.do(backups)


main()
