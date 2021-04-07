# not to confuse with __main__.py... meh
from pathlib import Path
from typing import Optional

from .sqlite import sqlite_process

import click


# TODO use context and default_map
# https://click.palletsprojects.com/en/7.x/commands/#overriding-defaults
def main(*, Normaliser, glob: str):
    @click.command()
    @click.argument('path', type=Path)
    @click.option('--dry', is_flag=True, default=True, show_default=True, help='Controls whether files will be actually deleted')
    @click.option('--max-workers', required=False, type=int, help='Passed down to ThreadPoolExecutore. Use 0 for serial execution')
    def make_main(path: Path, dry: bool, max_workers: Optional[int]) -> None:
        assert Normaliser is not None
        assert glob is not None
        # FIXME allow taking glob as option?
        # TODO collect all sqlite mimes?
        paths = list(sorted(path.rglob(glob)))
        # TODO support dry later
        sqlite_process(paths, Normaliser=Normaliser, max_workers=max_workers)
    mf = make_main()
    mf()
