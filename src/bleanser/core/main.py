# not to confuse with __main__.py... meh
from pathlib import Path
from typing import Optional, List

from .common import logger, Dry, Move, Remove, Mode
from .processor import compute_instructions, apply_instructions
from .utils import mime

import click


# TODO use context and default_map
# https://click.palletsprojects.com/en/7.x/commands/#overriding-defaults
def main(*, Normaliser) -> None:
    @click.group()
    def call_main() -> None:
        pass

    # TODO view mode?
    # might make easier to open without creating wals...
    # sqlite3 'file:places-20190731110302.sqlite?immutable=1' '.dump' | less

    @call_main.command(name='diff')
    @click.argument('path1', type=Path)
    @click.argument('path2', type=Path)
    def diff(path1: Path, path2: Path) -> None:
        from .processor import compute_diff
        # meh..
        for line in compute_diff(path1, path2, Normaliser=Normaliser):
            print(line)


    @call_main.command(name='clean')
    @click.argument('path', type=Path)
    @click.option('--dry', is_flag=True, default=None, show_default=True, help='Do not delete/move the input files, just print what would happen')
    @click.option('--move', type=Path, required=False, help='Path to move the redundant files  (safer than --remove mode)')
    @click.option('--remove', is_flag=True, default=None, show_default=True, help='Controls whether files will be actually deleted')
    @click.option('--max-workers', required=False, type=int, help='Passed down to ThreadPoolExecutore. Use 0 for serial execution')
    def clean(path: Path, dry: bool, move: Optional[Path], remove: bool, max_workers: Optional[int]) -> None:
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

        # TODO should also move to Noramliser?
        # todo not sure if this is the best way?
        SQLITE_MIME = 'application/x-sqlite3'
        paths = [
            p
            for p in list(sorted(path.rglob('*')))  # assumes sort order is same as date order? guess it's reasonable
            if p.is_file() and mime(p) == SQLITE_MIME
        ]
        logger.info('processing %d files (%s ... %s)', len(paths), paths[0], paths[-1])
        instructions = compute_instructions(paths, Normaliser=Normaliser, max_workers=max_workers)
        apply_instructions(instructions, mode=mode)
    call_main()
