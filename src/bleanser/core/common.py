from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from .ext.logging import LazyLogger

logger = LazyLogger(__name__, level='debug')


@dataclass
class Group:
    items: Sequence[Path]
    """
    All items in group are tied via 'domination' relationship
    Which might be either exact equality, or some sort of 'inclusion' relationship
    """

    pivots: Sequence[Path]
    """
    Pivots are the elements that 'define' group.
    In general the pivots contain all other elements in the group
    Sometimes pivots might be redundant, e.g. if we want to keep both boundaries of the group
    """

    # TODO attach diff or something
    # cmp: CmpResult
    error: bool

    def __post_init__(self) -> None:
        sp = set(self.pivots)
        si = set(self.items)
        if len(self.items) != len(si):
            raise RuntimeError(f'duplicate items: {self}')
        if len(self.pivots) != len(sp):
            raise RuntimeError(f'duplicate pivots: {self}')
        # in theory could have more pivots, but shouldn't happen for now
        assert 1 <= len(sp) <= 2, sp
        if not (sp <= si):
            raise RuntimeError(f"pivots aren't fully contained in items: {self}")


@dataclass
class Instruction:
    path: Path
    group: Group
    """
    'Reason' why the path got a certain instruction
    """


@dataclass
class Prune(Instruction):
    pass


@dataclass
class Keep(Instruction):
    pass


### helper to define paramertized tests in function's body
from .utils import under_pytest

if TYPE_CHECKING or under_pytest:
    import pytest

    parametrize = pytest.mark.parametrize
else:
    parametrize = lambda *_args, **_kwargs: (lambda f: f)
###


@dataclass
class BaseMode:
    pass


@dataclass
class Dry(BaseMode):
    pass


@dataclass
class Move(BaseMode):
    path: Path

    def __post_init__(self) -> None:
        assert self.path.is_dir(), self.path


@dataclass
class Remove(BaseMode):
    pass


type Mode = Dry | Move | Remove


def divide_by_size(*, buckets: int, paths: Sequence[Path]) -> Sequence[Sequence[Path]]:
    """
    Divide paths into approximately equally sized groups, while preserving order
    """
    res = []
    with_size = [(p, p.stat().st_size) for p in paths]
    bucket_size = sum(sz for _, sz in with_size) / buckets

    group: list[Path] = []
    group_size = 0

    def dump() -> None:
        nonlocal group_size, group

        if len(group) == 0:
            return

        res.append(group)
        # print(f"dumping group, size {group_size} {len(group)} {group[0]} {group[-1]}")

        group = []
        group_size = 0

    for p, sz in with_size:
        if group_size >= bucket_size:
            dump()
        group.append(p)
        group_size += sz
    # last group always needs to be dumped
    dump()

    assert len(res) <= buckets
    while len(res) < buckets:  # can be less if buckets > len(paths)
        res.append([])

    flattened = []
    for r in res:
        flattened.extend(r)
    assert paths == flattened, res  # just a safety check

    return res
