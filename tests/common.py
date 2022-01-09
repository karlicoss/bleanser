import os

import pytest

V = 'TEST_AS_KARLICOSS'

skip_if_not_karlicoss = pytest.mark.skipif(
    V not in os.environ, reason=f'test only works on @karlicoss data for now. Set env variable {V}=true to override.',
)


from pathlib import Path
TESTDATA = Path(__file__).absolute().parent / 'testdata'

from typing import List
from dataclasses import dataclass
@dataclass
class Res:
    cleaned  : List[Path]
    remaining: List[Path]


def actions(*, paths: List[Path], Normaliser, max_workers=0) -> Res:
    from bleanser.core.processor import compute_instructions
    from bleanser.core.common import Delete, Keep
    instructions = list(compute_instructions(paths, Normaliser=Normaliser, max_workers=max_workers))
    cleaned   = []
    remaining = []
    for i in instructions:
        if isinstance(i, Delete):  # todo rename delete to Clean?
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


def actions2(*, path: Path, rglob: str, Normaliser, max_workers=0) -> Res2:
    paths = list(sorted(path.rglob(rglob)))
    res = actions(paths=paths, Normaliser=Normaliser)
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
        # FIXME meh.. maybe instead instantiate an instance instead of class?
        setattr(Normaliser, key, value)
        yield
    finally:
        setattr(Normaliser, key, prev)
