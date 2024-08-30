# not to confuse with __main__.py... meh
from __future__ import annotations

import os
from glob import glob as do_glob
from pathlib import Path
from typing import cast

import click

from .common import Dry, Mode, Move, Remove, logger
from .processor import (
    BaseNormaliser,
    apply_instructions,
    bleanser_tmp_directory,
    compute_instructions,
)


# TODO use context and default_map
# https://click.palletsprojects.com/en/7.x/commands/#overriding-defaults
def main(*, Normaliser: type[BaseNormaliser]) -> None:
    # meh.. by default the width is stupid, like 80 chars
    context_settings = {
        'max_content_width': 120,
        'show_default': True,
    }

    @click.group(context_settings=context_settings)
    def call_main() -> None:
        pass

    # meh... would be nice to use object but it gets casted to str by click??
    _DEFAULT = '<default>'

    @call_main.command(name='diff', short_help='cleanup two files and diff')
    @click.argument('path1'          , type=str)
    @click.argument('path2'                        , default=_DEFAULT)
    @click.option  ('--glob'         , is_flag=True, default=False                   , help='Treat the path as glob (in the glob.glob sense)')
    @click.option  ('--vim'          , is_flag=True, default=False                   , help='Use vimdiff')
    @click.option  ('--difftool'     , type=str                                      , help='Custom difftool to use')
    @click.option  ('--from', 'from_', type=int    , default=None)
    @click.option  ('--to'           , type=int    , default=None                    , help='non-inclusive, i.e. [from, to)')
    def diff(path1: str, path2: Path, *, glob: bool, from_: int | None, to: int | None, vim: bool, difftool: str) -> None:
        path1_: Path
        if glob:
            assert path2 is cast(Path, _DEFAULT), path2
            if to is None:
                assert from_ is not None
                to = from_ + 2  # by default just compare with the next adjacent element
            paths = _get_paths(path=path1, from_=from_, to=to, glob=glob)
        else:
            assert cast(str, path2) is not _DEFAULT
            assert from_ is None
            assert to is None
            path1_ = Path(path1)
            path2 = Path(path2)
            paths = [path1_, path2]

        from .processor import compute_diff

        # meh..
        if vim:
            difftool = 'vimdiff'
        if difftool is not None:
            os.environ['DIFFTOOL'] = difftool

        for line in compute_diff(paths, Normaliser=Normaliser):
            print(line)

    @call_main.command(name='normalised', short_help='normalise file and dump to stdout')
    @click.argument('path', type=Path)
    @click.option('--stdout', is_flag=True, help='print normalised files to stdout instead of printing the path to it')
    def normalised(*, path: Path, stdout: bool) -> None:
        with bleanser_tmp_directory() as base_tmp_dir:
            n = Normaliser(original=path, base_tmp_dir=base_tmp_dir)
            with n.do_normalise() as cleaned:
                if stdout:
                    print(cleaned.read_text())
                else:
                    click.secho(f'You can examine normalised file: {cleaned}', fg='green')
                    click.pause(info="Press any key when you've finished")

    @call_main.command(name='prune', short_help='process & prune files')
    @click.argument('path', type=str)
    @click.option('--glob', is_flag=True, default=False, help='Treat the path as glob (in the glob.glob sense)')
    @click.option('--sort-by', type=click.Choice(['size', 'name']), default='name', help='how to sort input files')
    ##
    @click.option  ('--dry'   , is_flag=True, default=None, help='Do not prune the input files, just print what would happen after pruning.')
    @click.option  ('--remove', is_flag=True, default=None, help='Prune the input files by REMOVING them (be careful!)')
    @click.option  ('--move'  , type=Path                 , help='Prune the input files by MOVING them to the specified path. A bit safer than --remove mode.')
    ##
    @click.option('--yes', is_flag=True, default=False, help="Do not prompt before pruning files (useful for cron etc)")
    @click.option(
        '--threads',
        type=int, is_flag=False, flag_value=0, default=None,
        help="Number of threads (processes) to use. Without the flag won't use any, with the flag will try using all available, can also take a specific value. Passed down to PoolExecutor.",
    )
    ##
    @click.option  ('--from', 'from_', type=int    , default=None)
    @click.option  ('--to'           , type=int    , default=None)
    ##
    @click.option  ('--multiway'       , is_flag=True, default=None                , help='force "multiway" cleanup')
    @click.option  ('--prune-dominated', is_flag=True, default=None)
    def prune(*, path: str, sort_by: str, glob: bool, dry: bool, move: Path | None, remove: bool, threads: int | None, from_: int | None, to: int | None, multiway: bool | None, prune_dominated: bool | None, yes: bool) -> None:
        modes: list[Mode] = []
        if dry is True:
            modes.append(Dry())
        if move is not None:
            modes.append(Move(path=move))
        if remove is True:
            modes.append(Remove())
        if len(modes) == 0:
            modes.append(Dry())
        assert len(modes) == 1, f'please specify exactly one of modes (got {modes})'
        [mode] = modes
        # TODO eh, would be nice to use some package for mutually exclusive args..
        # e.g. https://stackoverflow.com/questions/37310718/mutually-exclusive-option-groups-in-python-click

        paths = _get_paths(path=path, glob=glob, from_=from_, to=to, sort_by=sort_by)

        if multiway is not None:
            Normaliser.MULTIWAY = multiway
        if prune_dominated is not None:
            Normaliser.PRUNE_DOMINATED = prune_dominated

        instructions = list(compute_instructions(paths, Normaliser=Normaliser, threads=threads))
        # NOTE: for now, forcing list() to make sure instructions compute before path check
        # not strictly necessary
        for p in paths:
            # just in case, to make sure no one messed with files in the meantime
            assert p.exists(), p

        need_confirm = not yes
        apply_instructions(instructions, mode=mode, need_confirm=need_confirm)
    call_main()


def _get_paths(*, path: str, from_: int | None, to: int | None, sort_by: str = "name", glob: bool=False) -> list[Path]:
    if not glob:
        pp = Path(path)
        assert pp.is_dir(), pp
        path = str(pp) + os.sep + '**'
    paths = [Path(p) for p in do_glob(path, recursive=True)]  # noqa: PTH207
    paths = [p for p in paths if p.is_file()]
    if sort_by == "name":
        # assumes sort order is same as date order? guess it's reasonable
        paths = sorted(paths)
    else:
        paths = sorted(paths, key=lambda s: s.stat().st_size)

    if from_ is None:
        from_ = 0
    if to is None:
        to = len(paths)
    paths = paths[from_:to]
    assert len(paths) > 0

    logger.info('processing %d files (%s ... %s)', len(paths), paths[0], paths[-1])
    return paths
