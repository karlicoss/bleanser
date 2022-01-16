# not to confuse with __main__.py... meh
from pathlib import Path
from typing import Optional, List

from .common import logger, Dry, Move, Remove, Mode
from .processor import compute_instructions, apply_instructions
from .utils import mime

import click # type: ignore


# TODO use context and default_map
# https://click.palletsprojects.com/en/7.x/commands/#overriding-defaults
def main(*, Normaliser) -> None:
    # meh.. by default the width is stupid, like 80 chars
    context_settings = {
        'max_content_width': 120,
        'show_default': True,
    }
    @click.group(context_settings=context_settings)
    def call_main() -> None:
        pass

    _DEFAULT = object()

    @call_main.command(name='diff', short_help='cleanup two files and diff')
    @click.argument('path1'          , type=Path)
    @click.argument('path2'                        , default=_DEFAULT)
    @click.option  ('--vim'          , is_flag=True, default=False                   , help='Use vimdiff')
    @click.option  ('--from', 'from_', type=int    , default=None    , required=False)
    @click.option  ('--to'           , type=int    , default=None    , required=False)
    def diff(path1: Path, path2: Path, *, from_: Optional[int], to: Optional[int], vim: bool) -> None:
        # TODO meh. how to do nargs='?' in click?
        if path2 is _DEFAULT:
            paths = _get_paths(path=path1, from_=from_, to=to)
            assert len(paths) == 2, paths
            [path1, path2] = paths
        else:
            path2 = Path(path2)

        from .processor import compute_diff
        # meh..
        if vim:
            import os
            os.environ['USE_VIMDIFF'] = 'yes'

        for line in compute_diff(path1, path2, Normaliser=Normaliser):
            print(line)

    # todo ugh, name sucks
    @call_main.command(name='cleaned', short_help='cleanup file and dump to stdout')
    @click.argument('path', type=Path)
    @click.option('--stdout', is_flag=True)
    def cleaned(path: Path, stdout: bool) -> None:
        n = Normaliser()
        from tempfile import TemporaryDirectory
        with TemporaryDirectory() as td, n.do_cleanup(path, wdir=Path(td)) as cleaned:
            if stdout:
                print(cleaned.read_text())
            else:
                click.secho(f'You can examine cleaned file: {cleaned}', fg='green')
                click.pause(info="Press any key when you've finished")


    @call_main.command(name='prune', short_help='process & prune files')
    @click.argument('path'              , type=Path)
    ##
    @click.option  ('--dry'             , is_flag=True, default=None                , help='Do not prune the input files, just print what would happen after pruning.')
    @click.option  ('--remove'          , is_flag=True, default=None                , help='Prune the input files by REMOVING them (be careful!)')
    @click.option  ('--move'            , type=Path                 , required=False, help='Prune the input files by MOVING them to the specified path. A bit safer than --remove mode.')
    ##
    @click.option(
        '--threads',
        type=int, is_flag=False, flag_value=0, default=None,
        required=False,
        help="Number of threads (processes) to use. Without the flag won't use any, with the flag will try using all available, can also take a specififc value. Passed down to PoolExecutor.",
    )
    ##
    @click.option  ('--from', 'from_'   , type=int    , default=None, required=False)
    @click.option  ('--to'              , type=int    , default=None, required=False)
    ##
    @click.option  ('--multiway'        , is_flag=True, default=None                , help='force "multiway" cleanup')
    @click.option  ('--prune-dominated' , is_flag=True, default=None)
    def prune(path: Path, dry: bool, move: Optional[Path], remove: bool, threads: Optional[int], from_: Optional[int], to: Optional[int], multiway: Optional[bool], prune_dominated: Optional[bool]) -> None:
        modes: List[Mode] = []
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

        paths = _get_paths(path=path, from_=from_, to=to)

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
        apply_instructions(instructions, mode=mode)
    call_main()


def _get_paths(*, path: Path, from_: Optional[int], to: Optional[int]) -> List[Path]:
    assert path.is_dir(), path
    # first try json...
    # meh... need to think how to support archived stuff properly?
    paths = list(sorted(path.rglob('*.json*')))
    # second, try sqlite
    if len(paths) == 0:
        SQLITE_MIME = 'application/x-sqlite3'
        # TODO might take a while if there are many paths
        paths = [p for p in paths if mime(p) == SQLITE_MIME]
    if len(paths) == 0:
        # TODO should also move to Normaliser?
        # todo not sure if this is the best way?
        paths = [
            p
            for p in list(sorted(path.rglob('*')))  # assumes sort order is same as date order? guess it's reasonable
            if p.is_file()
        ]

    if from_ is None:
        from_ = 0
    if to is None:
        to = len(paths)
    paths = paths[from_: to]
    assert len(paths) > 0

    logger.info('processing %d files (%s ... %s)', len(paths), paths[0], paths[-1])
    return paths
