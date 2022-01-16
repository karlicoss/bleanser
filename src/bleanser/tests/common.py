import os

import pytest

from pathlib import Path
TESTDATA = Path(__file__).absolute().parent / 'testdata'


def skip_if_no_data() -> None:
    if 'CI' in os.environ and not TESTDATA.exists():
        pytest.skip(f'test only works on @karlicoss private data for now')


from typing import List, Optional
from dataclasses import dataclass
@dataclass
class Res:
    pruned   : List[Path]
    remaining: List[Path]


def actions(*, paths: List[Path], Normaliser, threads: Optional[int]=None) -> Res:
    from bleanser.core.processor import compute_instructions
    from bleanser.core.common import Prune, Keep
    instructions = list(compute_instructions(paths, Normaliser=Normaliser, threads=threads))
    pruned    = []
    remaining = []
    for i in instructions:
        if isinstance(i, Prune):
            pruned.append(i.path)
        elif isinstance(i, Keep):
            remaining.append(i.path)
        else:
            raise RuntimeError(type(i))
    return Res(pruned=pruned, remaining=remaining)


@dataclass
class Res2:
    pruned   : List[str]
    remaining: List[str]


def actions2(*, path: Path, rglob: str, Normaliser, threads: Optional[int]=None) -> Res2:
    from bleanser.core.main import _get_paths
    pp = str(path) + os.sep + rglob
    paths = _get_paths(path=pp, glob=True, from_=None, to=None)
    res = actions(paths=paths, Normaliser=Normaliser, threads=threads)
    pruned   = res.pruned
    remaining = res.remaining
    return Res2(
        pruned   =[str(c.relative_to(path)) for c in pruned   ],
        remaining=[str(c.relative_to(path)) for c in remaining],
    )


from contextlib import contextmanager
@contextmanager
def hack_attribute(Normaliser, key, value):
    prev = getattr(Normaliser, key)
    try:
        # TODO meh.. maybe instead instantiate an instance instead of class?
        setattr(Normaliser, key, value)
        yield
    finally:
        setattr(Normaliser, key, prev)
