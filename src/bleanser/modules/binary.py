#!/usr/bin/env python3
"""
Format-agnostic, clean up as literal file diffs
"""
# TODO probably should give it a better name...

from bleanser.core.processor import BaseNormaliser


from contextlib import contextmanager
from pathlib import Path
import shutil
from typing import Iterator


class Normaliser(BaseNormaliser):
    # filter out additions; keep the rest
    DIFF_FILTER = '> '

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        path = path.absolute().resolve()
        cleaned_path = wdir / Path(*path.parts[1:]) / (path.name + '-cleaned')
        cleaned_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(path, cleaned_path)

        yield cleaned_path


if __name__ == '__main__':
    Normaliser.main()
