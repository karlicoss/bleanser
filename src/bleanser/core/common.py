from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import NamedTuple, Sequence, Set, List, Iterator, Tuple, Dict, Iterable, Optional, Union

from .utils import assert_never
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


class Config(NamedTuple):
    prune_dominated: bool = False
    multiway       : bool = False


### helper to define paramertized tests in function's body
from .utils import under_pytest
if under_pytest:
    import pytest  # type: ignore
    parametrize = pytest.mark.parametrize
else:
    parametrize = lambda *args,**kwargs: (lambda f: f)  # type: ignore
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

@dataclass
class Remove(BaseMode):
    pass


Mode = Union[Dry, Move, Remove]
