from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest

TESTDATA = Path(__file__).absolute().parent / 'testdata'


def skip_if_no_data() -> None:
    if 'CI' in os.environ and not TESTDATA.exists():
        pytest.skip('test only works on @karlicoss private data for now')


@dataclass
class Res:
    pruned: list[Path]
    remaining: list[Path]


def actions(*, paths: list[Path], Normaliser, threads: int | None = None) -> Res:
    from bleanser.core.common import Keep, Prune
    from bleanser.core.processor import compute_instructions

    instructions = list(compute_instructions(paths, Normaliser=Normaliser, threads=threads))
    pruned = []
    remaining = []
    for i in instructions:
        if isinstance(i, Prune):
            pruned.append(i.path)
        elif isinstance(i, Keep):
            remaining.append(i.path)
        else:
            raise TypeError(type(i))
    return Res(pruned=pruned, remaining=remaining)


@dataclass
class Res2:
    pruned: list[str]
    remaining: list[str]


def actions2(*, path: Path, rglob: str, Normaliser, threads: int | None = None) -> Res2:
    from bleanser.core.cli import _get_paths

    pp = str(path) + os.sep + rglob
    paths = _get_paths(path=pp, glob=True, from_=None, to=None)
    res = actions(paths=paths, Normaliser=Normaliser, threads=threads)
    pruned = res.pruned
    remaining = res.remaining
    return Res2(
        pruned   =[str(c.relative_to(path)) for c in pruned   ],
        remaining=[str(c.relative_to(path)) for c in remaining],
    )  # fmt: skip


@contextmanager
def hack_attribute(Normaliser, key, value):
    prev = getattr(Normaliser, key)
    try:
        # TODO meh.. maybe instead instantiate an instance instead of class?
        setattr(Normaliser, key, value)
        yield
    finally:
        setattr(Normaliser, key, prev)
