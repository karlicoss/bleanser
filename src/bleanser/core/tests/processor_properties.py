"""
Property-based tests for the pruning algorithm in bleanser.core.processor.

model_keep is a small reference implementation of the current grouping semantics, operating directly on sets.
Hypothesis generates synthetic export histories and materialises them as files.
The real compute_instructions, FileSet, sort, diff, and cmp pipeline must match the model exactly.
The model doubles as an executable specification of compute_groups, so it favours simplicity over efficiency.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal, get_args

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from ..common import Instruction, Keep
from ..processor import BaseNormaliser, compute_instructions
from .processor_property_checks import Snapshot, assert_instruction_safety

_ERROR = RuntimeError('simulated normalisation failure')


def model_keep(snapshots: list[Snapshot], *, multiway: bool, prune_dominated: bool) -> list[bool]:
    """
    Which files does bleanser keep? Returns one bool per input snapshot.

    Snapshots are processed left to right, greedily growing a window while the window invariant holds.
    When the window can't grow anymore, its two boundaries ('pivots') are kept and everything in between is pruned.
    Both boundaries are kept even when the window is a pair, so a window has to reach size 3 to prune anything.
    Windows overlap: the right pivot of one window becomes the left pivot of the next.
    """
    if multiway:
        assert prune_dominated  # the real implementation asserts this combination too

    n = len(snapshots)
    keep = [False] * n
    left = 0
    while left < n:
        lset = snapshots[left]
        if isinstance(lset, Exception):
            # errored files are always kept
            keep[left] = True
            left += 1
            continue
        window = lset  # union of all snapshots in the window so far
        prev = lset  # rightmost snapshot in the window so far
        right = left
        while right + 1 < n:
            new = snapshots[right + 1]
            if isinstance(new, Exception):
                # errored files break the window
                break
            if multiway:
                # the whole window must be covered by its two boundaries
                ok = window <= (lset | new)
            else:
                # two-way: each snapshot must contain (or equal) the previous one
                ok = prev <= new if prune_dominated else prev == new
            if not ok:
                break
            window |= new
            prev = new
            right += 1
        keep[left] = True
        keep[right] = True
        # if the window didn't grow, just move on; otherwise the right pivot starts the next window
        left = left + 1 if right == left else right
    return keep


def test_model_on_known_examples() -> None:
    # These hand-checked cases anchor the model independently of the production implementation.
    # They mirror test_twoway and test_multiway in processor.py so the two specifications stay aligned.
    data: list[Snapshot] = [
        frozenset({'X'}),
        frozenset({'B'}),
        frozenset({'B'}),
        frozenset({'B'}),
        frozenset({'B', 'A'}),
        frozenset({'C', 'B', 'A'}),
        frozenset({'A', 'BB', 'C'}),
        frozenset({'B', 'A', 'E', 'Y'}),
    ]
    _Keep, _Prune = True, False
    assert model_keep(data, multiway=False, prune_dominated=True ) == [
        _Keep,   # X is not contained in B.
        _Keep,   # Left pivot of the B-to-ABC domination chain.
        _Prune,  # B == B
        _Prune,  # B == B
        _Prune,  # AB is dominated by ABC.
        _Keep,   # Right pivot of the B-to-ABC domination chain.
        _Keep,   # Replacing B with BB breaks the domination chain.
        _Keep,   # Final file.
    ]  # fmt: skip
    assert model_keep(data, multiway=False, prune_dominated=False) == [
        _Keep,   # X differs from B.
        _Keep,   # Left boundary of the identical-B run.
        _Prune,  # Interior of the identical-B run.
        _Keep,   # Right boundary of the identical-B run.
        _Keep,   # AB differs from B and ABC.
        _Keep,   # ABC differs from both neighbours.
        _Keep,   # A/BB/C differs from both neighbours.
        _Keep,   # Final file.
    ]  # fmt: skip

    extra: list[Snapshot] = [
        frozenset({'00', '11', '22'}),
        frozenset({'11', '22', '33', '44'}),
        frozenset({'22', '33', '44', '55'}),
        frozenset({'44', '55', '66'}),
        frozenset({'55', '66'}),
    ]
    assert model_keep(data + extra, multiway=True, prune_dominated=True) == [
        _Keep,   # Left pivot covering the next four files together with ABC.
        _Prune,  # B is covered by X and ABC.
        _Prune,  # B is covered by X and ABC.
        _Prune,  # B is covered by X and ABC.
        _Prune,  # AB is covered by X and ABC.
        _Keep,   # Right pivot of the first multiway window.
        _Keep,   # BB prevents the following file from extending this window.
        _Keep,   # Its B/E/Y atoms are not covered if the right pivot advances to 00/11/22.
        _Keep,   # Left pivot of the first rolling window.
        _Prune,  # Covered by the neighbouring 00/11/22 and 22/33/44/55 pivots.
        _Keep,   # Shared pivot between the two rolling windows.
        _Prune,  # Covered by the neighbouring 22/33/44/55 and 55/66 pivots.
        _Keep,   # Final pivot.
    ]  # fmt: skip


_HEADER = '#header'  # first line of every input file, so inputs are never empty (BaseNormaliser asserts size > 0)
_BOOM = 'BOOM'  # magic line making the test normaliser raise


def _run_real(
    snapshots: list[Snapshot], *, multiway: bool, prune_dominated: bool
) -> tuple[list[Path], list[Instruction]]:
    """
    Materialises abstract snapshots as files and runs them through the real pipeline.
    """

    class TestNormaliser(BaseNormaliser):
        MULTIWAY = multiway
        PRUNE_DOMINATED = prune_dominated

        @contextmanager
        def normalise(self, *, path: Path) -> Iterator[Path]:
            lines = path.read_text().splitlines()
            assert lines[0] == _HEADER
            items = lines[1:]
            if _BOOM in items:
                raise RuntimeError('simulated normalisation failure')
            normalised = self.tmp_dir / 'normalised'
            normalised.write_text(''.join(i + '\n' for i in items))
            yield normalised

    with TemporaryDirectory() as td:
        tdir = Path(td)
        paths = []
        for i, snapshot in enumerate(snapshots):
            lines = [_HEADER, *([_BOOM] if isinstance(snapshot, Exception) else sorted(snapshot))]
            # Reverse-lexical names make the order assertion detect accidental filename sorting.
            p = tdir / f'{len(snapshots) - i:04}.txt'
            p.write_text(''.join(l + '\n' for l in lines))
            paths.append(p)
        instructions = list(compute_instructions(paths, Normaliser=TestNormaliser, threads=None))

    return paths, instructions


# tiny universe of atoms, so that equal/subset/overlapping snapshots happen all the time
ATOMS = st.sampled_from('abcdef')
SNAPSHOTS = st.frozensets(ATOMS)
type Operation = Literal[
    'repeat',   # Reuse the previous snapshot unchanged.
    'grow',     # Retain every old atom and add any new atoms.
    'shift',    # Drop any old atoms and add any new atoms.
    'shrink',   # Remove any subset of the previous atoms without adding new ones.
    'rewrite',  # Generate a new snapshot independently of the previous one.
    'error',    # Simulate an exporter or normalisation failure.
]  # fmt: skip
_OPERATIONS: tuple[Operation, ...] = (*get_args(Operation.__value__), 'grow')


def _subsets_of(s: frozenset[str]) -> st.SearchStrategy[frozenset[str]]:
    if len(s) == 0:
        # sampled_from rejects an empty sequence, whose only possible subset is itself.
        return st.just(frozenset())
    return st.frozensets(st.sampled_from(sorted(s)))


@st.composite
def export_histories(draw: st.DrawFn) -> list[Snapshot]:
    """
    Sequences shaped like real export histories: repeats, growth, rolling windows, retention, rewrites, failures.
    """
    prev = draw(SNAPSHOTS)
    history: list[Snapshot] = [prev]
    for _ in range(draw(st.integers(0, 15))):
        op = draw(st.sampled_from(_OPERATIONS))
        if op == 'error':
            history.append(_ERROR)
            continue
        new: frozenset[str]
        if op == 'repeat':
            new = prev
        elif op == 'grow':
            new = prev | draw(SNAPSHOTS)
        elif op == 'shift':
            new = draw(_subsets_of(prev)) | draw(SNAPSHOTS)
        elif op == 'shrink':
            new = draw(_subsets_of(prev))
        else:
            assert op == 'rewrite', op
            new = draw(SNAPSHOTS)
        history.append(new)
        prev = new
    return history


# plain random sequences to cover adversarial cases the walk above is unlikely to produce
HISTORIES = export_histories() | st.lists(SNAPSHOTS | st.just(_ERROR), min_size=1, max_size=16)


@pytest.mark.parametrize(
    ('multiway', 'prune_dominated'),
    [
        pytest.param(False, False, id='twoway-exact'),
        pytest.param(False, True , id='twoway-dominated'),
        pytest.param(True , True , id='multiway'),
    ],
)  # fmt: skip
@given(snapshots=HISTORIES)
# a few known corner cases, always checked before random exploration
@example(snapshots=[frozenset()])
@example(snapshots=[frozenset({'a'}), frozenset({'a'})])
@example(snapshots=[frozenset({'a'}), _ERROR, frozenset({'a'})])
@example(snapshots=[frozenset({'a'}), frozenset(), frozenset({'a'})])
@example(snapshots=[frozenset({'a', 'b'}), frozenset({'b', 'c'}), frozenset({'c', 'd'})])
# database=None to avoid crapping .hypothesis dir into the project; failures are printed anyway
@settings(max_examples=60, deadline=None, database=None)
def test_real_matches_model(*, multiway: bool, prune_dominated: bool, snapshots: list[Snapshot]) -> None:
    expected           = model_keep(snapshots, multiway=multiway, prune_dominated=prune_dominated)  # fmt: skip
    paths, instructions = _run_real(snapshots, multiway=multiway, prune_dominated=prune_dominated)
    actual = [isinstance(instruction, Keep) for instruction in instructions]

    # The real file-based pipeline must make exactly the same keep/prune decisions as the small set-based model.
    assert actual == expected

    # These checks state safety properties independently of the reference model.
    # This prevents a matching bug in the model and implementation from making the test pass.
    assert_instruction_safety(snapshots=snapshots, paths=paths, instructions=instructions)
