from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import NamedTuple, Sequence, Set, List, Iterator, Tuple

from .utils import assert_never

from more_itertools import pairwise


# meh. get rid of this...
from kython.klogging2 import LazyLogger
logger = LazyLogger(__name__, level='debug')


class CmpResult(Enum):
    DIFFERENT = 'different'
    SAME      = 'same'
    DOMINATES = 'dominates'
    ERROR     = 'error'


class Diff(NamedTuple):
    cmp: CmpResult
    diff: bytes


class Relation(NamedTuple):
    before: Path
    diff: Diff
    after: Path


@dataclass
class Instruction:
    path: Path
    relation: Relation  # kind of 'reason'? not sure if useful..


@dataclass
class Delete(Instruction):
    pass

@dataclass
class Keep(Instruction):
    pass


class Config(NamedTuple):
    delete_dominated: bool = False


def relations_to_instructions(relations: Sequence[Relation], *, config: Config=Config()) -> Sequence[Instruction]:
    assert len(relations) > 0  # not sure...
    # NOTE: using Sequence, not Iterator to ensure more atomic behaviour/earlier sanity checks

    def it() -> Iterator[Instruction]:
        ## sanity pre-check
        seq: List[Path] = []
        sames: List[Tuple[Path, Relation]] = []
        def dump_group() -> Iterator[Instruction]:
            nonlocal sames
            for i, s in enumerate(sames):
                (path, relation) = s
                A = Keep if i == 0 or i == len(sames) - 1 else Delete
                yield A(path=path, relation=relation)
            sames.clear()

        for r in relations:
            if len(seq) == 0:
                seq.append(r.before)  # first item
            prev = seq[-1]
            assert prev == r.before
            if r.after in seq:
                raise RuntimeError(f'duplicate path {r.after}')
            seq.append(r.after)

            sames.append((prev, r))

            res = r.diff.cmp
            if res == CmpResult.DOMINATES:
                res = CmpResult.SAME if config.delete_dominated else CmpResult.DIFFERENT

            if res == CmpResult.ERROR:
                # error is useful to distinguish for debugging purposes.. but as far as bleanser concerned it's the same
                res = CmpResult.DIFFERENT

            if   res is CmpResult.DIFFERENT:
                # dump previous
                yield from dump_group()
            elif res is CmpResult.SAME:
                # no-op, just add to the group
                pass
            elif res is CmpResult.ERROR or res is CmpResult.DOMINATES:
                raise RuntimeError(f"shouldn't happen {res}")
            else:
                assert_never(res)
        last = seq[-1]
        sames.append((last, r))  # fixme reusing relation...
        yield from dump_group()

        # always keep last one?
        # on the other hand might be beneficial to keep the first one instead if they all are same... more cache friendly
        # yield Instruction(path=last, action='keep', relation=r)

    res = list(it())
    # breakpoint()
    assert len(res) == len(relations) + 1, (relations, res)
    return  res


def test_relations_to_instructions() -> None:
    def do(*pp, config=Config()):
        args = (Relation(before=b, after=a, diff=Diff(cmp=r, diff=b'')) for b, a, r in pp)
        res = relations_to_instructions(list(args), config=config)
        return [(p.path, {Keep: 'keep', Delete: 'delete'}[type(p)]) for p in res]

    CR = CmpResult

    assert do(
        ('a', 'b', CR.DIFFERENT),
    ) == [
        ('a', 'keep'),
        ('b', 'keep'),
    ]

    assert do(
        ('0', 'a', CR.DIFFERENT),
        ('a', 'b', CR.SAME     ),
        ('b', 'c', CR.SAME     ),
        ('c', 'd', CR.SAME     ),
    ) == [
        ('0', 'keep'  ),
        ('a', 'keep'  ),
        ('b', 'delete'),
        ('c', 'delete'),
        ('d', 'keep'  ),
    ]


    inputs = [
        ('a', 'b', CR.SAME     ),
        ('b', 'c', CR.DIFFERENT),
        ('c', 'd', CR.DOMINATES),
        ('d', 'e', CR.SAME     ),
        ('e', 'f', CR.DOMINATES),
        ('f', 'g', CR.DIFFERENT),
        ('g', 'h', CR.SAME     ),
    ]

    assert do(*inputs) == [
        ('a', 'keep'  ),
        ('b', 'keep'  ),
        ('c', 'keep'  ),
        ('d', 'keep'  ),
        ('e', 'keep'  ),
        ('f', 'keep'  ),
        ('g', 'keep'  ),
        ('h', 'keep'  ),
    ]

    assert do(*inputs, config=Config(delete_dominated=True)) == [
        ('a', 'keep'  ),
        ('b', 'keep'  ),
        ('c', 'keep'  ),
        ('d', 'delete'),
        ('e', 'delete'),
        ('f', 'keep'  ),
        ('g', 'keep'  ),
        ('h', 'keep'  ),
    ]

    import pytest  # type: ignore
    with pytest.raises(RuntimeError, match='duplicate'):
        do(
            ('a', 'b', CR.DIFFERENT),
            ('b', 'a', CR.DIFFERENT),
        )

    with pytest.raises(AssertionError):
        do(
            ('a', 'b', CR.DIFFERENT),
            ('c', 'd', CR.DIFFERENT),
        )

    with pytest.raises(AssertionError):
        do(
            ('a', 'b', CR.DIFFERENT),
            ('c', 'c', CR.DIFFERENT),
            ('d', 'e', CR.DIFFERENT),
        )
