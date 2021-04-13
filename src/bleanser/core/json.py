#!/usr/bin/env python3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from bleanser.core.common import logger
from bleanser.core.utils import Json
from bleanser.core.sqlite import BaseNormaliser


class JsonNormaliser(BaseNormaliser):
    # compare as is
    DIFF_FILTER = None

    def cleanup(self, j: Json) -> None:
        # TODO not sure if should modify in place?
        pass

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        # todo copy paste from SqliteNormaliser
        assert path.is_absolute(), path
        cleaned = wdir / Path(*path.parts[1:]) / (path.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy(path, cleaned)

        import json
        with cleaned.open('r') as fp:
            j = json.load(fp)
        self.cleanup(j)
        with cleaned.open('w') as fp:
            json.dump(j, fp=fp, indent=2, sort_keys=True)
        yield cleaned


def delkey(j: Json, *, key: str) -> None:
    # todo if primitive, don't do anything
    if   isinstance(j, (int, float, bool, type(None), str)):
        return
    elif isinstance(j, list):
        for v in j:
            delkey(v, key=key)
    elif isinstance(j, dict):
        j.pop(key, None)
        for k, v in j.items():
            delkey(v, key=key)
    else:
        raise RuntimeError(type(j))


# can work as generic json processor
if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=JsonNormaliser)
