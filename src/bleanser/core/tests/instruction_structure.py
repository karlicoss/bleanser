from pathlib import Path

import pytest

from ..common import Group, Keep, Prune, Remove
from ..processor import apply_instructions, assert_instruction_structure


def _shared_pivot_plan() -> tuple[list[Path], list[Keep | Prune]]:
    first, middle, shared, last = map(Path, ('first', 'middle', 'shared', 'last'))
    left_group  = Group(items=[first, middle, shared], pivots=[first, shared], error=False)  # fmt: skip
    right_group = Group(items=[shared, last], pivots=[shared, last], error=False)
    instructions: list[Keep | Prune] = [
        Keep(path=first  , group=left_group),
        Prune(path=middle, group=left_group),
        Keep(path=shared , group=left_group),
        Keep(path=last   , group=right_group),
    ]  # fmt: skip
    return [first, middle, shared, last], instructions


def test_accepts_overlapping_groups_with_a_shared_pivot() -> None:
    paths, instructions = _shared_pivot_plan()
    assert_instruction_structure(paths=paths, instructions=instructions)


def test_rejects_instructions_in_a_different_order() -> None:
    paths, instructions = _shared_pivot_plan()
    instructions.reverse()

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=paths, instructions=instructions)


def test_rejects_a_keep_for_a_non_pivot() -> None:
    paths, instructions = _shared_pivot_plan()
    middle = paths[1]
    # The fixture prunes "middle" because it is not a pivot; replace that instruction with an invalid Keep.
    instructions[1] = Keep(path=middle, group=instructions[1].group)

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=paths, instructions=instructions)


def test_rejects_a_non_contiguous_group() -> None:
    first, gap, middle, last = map(Path, ('first', 'gap', 'middle', 'last'))
    # The outer group skips "gap" in the input order, so its items do not form a contiguous slice.
    outer_group = Group(items=[first, middle, last], pivots=[first, last], error=False)
    gap_group   = Group(items=[gap], pivots=[gap], error=False)  # fmt: skip
    instructions: list[Keep | Prune] = [
        Keep(path=first  , group=outer_group),
        Keep(path=gap    , group=gap_group),
        Prune(path=middle, group=outer_group),
        Keep(path=last   , group=outer_group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, gap, middle, last], instructions=instructions)


def test_rejects_non_boundary_pivots() -> None:
    first, middle, last = map(Path, ('first', 'middle', 'last'))
    # The group incorrectly uses the interior "middle" item as its right pivot instead of the "last" boundary.
    group = Group(items=[first, middle, last], pivots=[first, middle], error=False)
    instructions: list[Keep | Prune] = [
        Keep(path=first , group=group),
        Keep(path=middle, group=group),
        Prune(path=last , group=group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, middle, last], instructions=instructions)


def test_rejects_a_multi_item_error_group() -> None:
    first, last = map(Path, ('first', 'last'))
    error_group = Group(items=[first, last], pivots=[first, last], error=True)
    instructions: list[Keep | Prune] = [
        Keep(path=first, group=error_group),
        Keep(path=last , group=error_group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, last], instructions=instructions)


def test_rejects_a_pruned_neighbour_of_an_error() -> None:
    first, previous, error, last = map(Path, ('first', 'previous', 'error', 'last'))
    # Making the adjacent path an interior prune requires a malformed group spanning the error.
    left_group  = Group(items=[first, previous, error], pivots=[first, error], error=False)  # fmt: skip
    error_group = Group(items=[error], pivots=[error], error=True)
    right_group = Group(items=[last], pivots=[last], error=False)
    instructions: list[Keep | Prune] = [
        Keep(path=first    , group=left_group),
        Prune(path=previous, group=left_group),
        Keep(path=error    , group=error_group),
        Keep(path=last     , group=right_group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, previous, error, last], instructions=instructions)


def test_rejects_a_successful_group_crossing_an_error() -> None:
    first, error, last = map(Path, ('first', 'error', 'last'))
    spanning_group = Group(items=[first, error, last], pivots=[first, last], error=False)
    error_group    = Group(items=[error], pivots=[error], error=True)  # fmt: skip
    instructions: list[Keep | Prune] = [
        Keep(path=first, group=spanning_group),
        Keep(path=error, group=error_group),
        Keep(path=last , group=spanning_group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, error, last], instructions=instructions)


def test_rejects_a_path_pruned_in_one_group_but_used_as_another_group_pivot() -> None:
    first, conflict, shared, last = map(Path, ('first', 'conflict', 'shared', 'last'))
    left_group  = Group(items=[first, conflict, shared], pivots=[first, shared], error=False)  # fmt: skip
    right_group = Group(items=[conflict, shared, last], pivots=[conflict, last], error=False)
    instructions: list[Keep | Prune] = [
        Keep(path=first    , group=left_group),
        Prune(path=conflict, group=left_group),
        Keep(path=shared   , group=left_group),
        Keep(path=last     , group=right_group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, conflict, shared, last], instructions=instructions)


def test_rejects_a_path_kept_in_one_group_but_used_as_another_group_non_pivot() -> None:
    first, conflict, shared, last = map(Path, ('first', 'conflict', 'shared', 'last'))
    left_group  = Group(items=[first, conflict, shared], pivots=[first, shared], error=False)  # fmt: skip
    right_group = Group(items=[conflict, shared, last], pivots=[conflict, last], error=False)
    instructions: list[Keep | Prune] = [
        Keep(path=first   , group=left_group),
        Keep(path=conflict, group=right_group),
        Keep(path=shared  , group=left_group),
        Keep(path=last    , group=right_group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        assert_instruction_structure(paths=[first, conflict, shared, last], instructions=instructions)


def test_apply_checks_structure_before_reading_or_modifying_files(tmp_path: Path) -> None:
    first  = tmp_path / 'first'  # fmt: skip
    middle = tmp_path / 'middle'
    last   = tmp_path / 'last'  # fmt: skip

    group = Group(items=[first, middle, last], pivots=[first, last], error=False)
    instructions: list[Keep | Prune] = [
        Keep(path=first , group=group),
        Keep(path=middle, group=group),
        Keep(path=last  , group=group),
    ]  # fmt: skip

    with pytest.raises(AssertionError):
        apply_instructions(
            instructions,
            paths=[first, middle, last],
            mode=Remove(),
            need_confirm=False,
            prune_empty_dirs=False,
        )
