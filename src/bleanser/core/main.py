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
    @click.group()
    def call_main() -> None:
        pass

    # TODO view mode?
    # might make easier to open without creating wals...
    # sqlite3 'file:places-20190731110302.sqlite?immutable=1' '.dump' | less

    @call_main.command(name='diff', short_help='diff two files after cleanup')
    @click.option('--vim', is_flag=True, default=False, show_default=True, help='Use vimdiff')
    @click.argument('path1', type=Path)
    @click.argument('path2', type=Path)
    def diff(path1: Path, path2: Path, *, vim: bool) -> None:
        from .processor import compute_diff
        # meh..
        if vim:
            import os
            os.environ['USE_VIMDIFF'] = 'yes'

        for line in compute_diff(path1, path2, Normaliser=Normaliser):
            print(line)

    # todo ugh, name sucks
    @call_main.command(name='cleaned', short_help='dump file after cleanup to stdout')
    @click.argument('path', type=Path)
    @click.option('--stdout', is_flag=True)
    def cleaned(path: Path, stdout: bool) -> None:
        n = Normaliser()
        from tempfile import TemporaryDirectory
        # TODO might be nice to print time...
        # TODO for json, we want to print the thing after jq processing? hmm
        with TemporaryDirectory() as td, n.do_cleanup(path, wdir=Path(td)) as cleaned:
            if stdout:
                print(cleaned.read_text())
            else:
                click.secho(f'You can examine cleaned file: {cleaned}', fg='green')
                click.pause(info="Press any key when you've finished")


    @call_main.command(name='clean', short_help='process & cleanup files')
    @click.argument('path', type=Path)
    @click.option('--dry', is_flag=True, default=None, show_default=True, help='Do not delete/move the input files, just print what would happen')
    @click.option('--move', type=Path, required=False, help='Path to move the redundant files  (safer than --remove mode)')
    @click.option('--remove', is_flag=True, default=None, show_default=True, help='Controls whether files will be actually deleted')
    @click.option('--max-workers', required=False, type=int, help='Passed down to PoolExecutor. Use 0 for serial execution')
    # todo originally, hack for memory leak issue
    # TODO ugh. how to name it --from? metavar= didn't work
    @click.option('--from_', required=False, type=int, default=None)
    @click.option('--to'  , required=False, type=int, default=None)
    @click.option('--multiway', is_flag=True, default=None, help='force "multiway" cleanup')
    @click.option('--delete-dominated', is_flag=True, default=None)
    def clean(path: Path, dry: bool, move: Optional[Path], remove: bool, max_workers: Optional[int], from_: Optional[int], to: Optional[int], multiway: Optional[bool], delete_dominated: Optional[bool]) -> None:
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
        if multiway is not None:
            Normaliser.MULTIWAY = multiway
        if delete_dominated is not None:
            Normaliser.DELETE_DOMINATED = delete_dominated

        instructions = compute_instructions(paths, Normaliser=Normaliser, max_workers=max_workers)
        apply_instructions(instructions, mode=mode)
    call_main()
