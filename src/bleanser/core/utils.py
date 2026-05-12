from __future__ import annotations

from pathlib import Path


def total_dir_size(d: Path) -> int:
    return sum(f.stat().st_size for f in d.glob('**/*') if f.is_file())


import sys

under_pytest = 'pytest' in sys.modules
### ugh. pretty horrible... but
# 'PYTEST_CURRENT_TEST' in os.environ
# doesn't work before we're actually inside the test.. and it might be late for decorators, for instance
###


import time


class Timer:
    def __init__(self, *tags):
        self.tags = tags

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        delta = self.end - self.start
        print(f"{self.tags} TIME TAKEN: {delta:.1f}", file=sys.stderr)


from functools import wraps


def timing(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        with Timer(f.__name__):
            return f(*args, **kwargs)

    return wrapped


from collections.abc import Collection
from typing import Any

Json = Any


def delkeys(j: Json, *, keys: str | Collection[str]) -> None:
    if isinstance(keys, str):
        keys = {keys}  # meh

    # todo if primitive, don't do anything
    if isinstance(j, (int, float, bool, type(None), str)):
        return
    elif isinstance(j, list):
        for v in j:
            delkeys(v, keys=keys)
    elif isinstance(j, dict):
        for key in keys:
            j.pop(key, None)
        for v in j.values():
            delkeys(v, keys=keys)
    else:
        raise TypeError(type(j))


def patch_atoms(j: Json, *, patch):
    if isinstance(j, (int, float, bool, type(None), str)):
        return patch(j)
    elif isinstance(j, list):
        for i in range(len(j)):
            j[i] = patch_atoms(j[i], patch=patch)
        return j
    elif isinstance(j, dict):
        for k in list(j.keys()):
            j[k] = patch_atoms(j[k], patch=patch)
        return j
    else:
        raise TypeError(type(j))
