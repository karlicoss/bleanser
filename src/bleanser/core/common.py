from enum import Enum
from pathlib import Path
from typing import NamedTuple


class CmpResult(Enum):
    DIFFERENT = 'different'
    SAME = 'same'
    DOMINATES = 'dominates'
    ERROR = 'error'  # FIXME need to handle it?


class Diff(NamedTuple):
    cmp: CmpResult
    diff: bytes


class Relation(NamedTuple):
    before: Path
    diff: Diff
    after: Path



# meh. get rid of this...
from kython.klogging2 import LazyLogger
logger = LazyLogger(__name__, level='debug')
