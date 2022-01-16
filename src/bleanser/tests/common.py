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
    cleaned  : List[Path]
    remaining: List[Path]


def actions(*, paths: List[Path], Normaliser, threads: Optional[int]=None) -> Res:
    from bleanser.core.processor import compute_instructions
    from bleanser.core.common import Prune, Keep
    instructions = list(compute_instructions(paths, Normaliser=Normaliser, threads=threads))
    cleaned   = [] # FIXME rename to pruned
    remaining = []
    for i in instructions:
        if isinstance(i, Prune):
            cleaned.append(i.path)
        elif isinstance(i, Keep):
            remaining.append(i.path)
        else:
            raise RuntimeError(type(i))
    return Res(cleaned=cleaned, remaining=remaining)


@dataclass
class Res2:
    cleaned  : List[str]
    remaining: List[str]


def actions2(*, path: Path, rglob: str, Normaliser, threads: Optional[int]=None) -> Res2:
    paths = list(sorted(path.rglob(rglob)))
    res = actions(paths=paths, Normaliser=Normaliser, threads=threads)
    cleaned   = res.cleaned
    remaining = res.remaining
    return Res2(
        cleaned  =[str(c.relative_to(path)) for c in cleaned  ],
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
