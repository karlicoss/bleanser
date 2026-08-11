"""Safety assertions shared by serial and process-pool processor properties."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from ..common import Instruction, Keep, Prune
from ..processor import assert_instruction_structure

# One snapshot is the set of lines in its normalised dump, while an Exception represents a normalisation failure.
type Snapshot = frozenset[str] | Exception


def assert_instruction_safety(
    *,
    snapshots: Sequence[Snapshot],
    paths: Sequence[Path],
    instructions: Sequence[Instruction],
) -> None:
    assert_instruction_structure(paths=paths, instructions=instructions)

    assert len(snapshots) == len(paths)

    keep_by_path = {instruction.path: isinstance(instruction, Keep) for instruction in instructions}
    snapshots_by_path = dict(zip(paths, snapshots, strict=True))

    # Several instructions reference the same group object, while adjacent groups may share a pivot.
    # Deduplicating by identity retains distinct instruction-visible groups.
    groups = list({id(instruction.group): instruction.group for instruction in instructions}.values())
    for group in groups:
        if group.error:
            assert isinstance(snapshots_by_path[group.items[0]], Exception)
        else:
            # Successful groups cannot cross or absorb an opaque normalisation failure.
            for item in group.items:
                assert not isinstance(snapshots_by_path[item], Exception)

    # A failed normalisation is opaque, so it and its immediate neighbours form a retained barrier.
    for index, snapshot in enumerate(snapshots):
        if not isinstance(snapshot, Exception):
            continue
        for neighbour in (index - 1, index, index + 1):
            if 0 <= neighbour < len(paths):
                assert keep_by_path[paths[neighbour]]

    # A prune decision is justified only by successful pivots in that same group covering all of its atoms.
    for instruction in instructions:
        if not isinstance(instruction, Prune):
            continue
        snapshot = snapshots_by_path[instruction.path]
        assert not isinstance(snapshot, Exception)
        pivot_union: set[str] = set()
        for pivot in instruction.group.pivots:
            pivot_snapshot = snapshots_by_path[pivot]
            assert not isinstance(pivot_snapshot, Exception)
            pivot_union.update(pivot_snapshot)
        assert snapshot <= pivot_union

    # Across all groups and chunks, pruning must preserve every atom from every successfully normalised input.
    successful_union: set[str] = set()
    kept_union: set[str] = set()
    for path, snapshot in zip(paths, snapshots, strict=True):
        if isinstance(snapshot, Exception):
            continue
        successful_union.update(snapshot)
        if keep_by_path[path]:
            kept_union.update(snapshot)
    assert kept_union == successful_union
