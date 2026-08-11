"""Property tests for source-file and process-pool safety guarantees."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from hypothesis import example, given, settings
from hypothesis import strategies as st

from ..common import Keep, Prune, divide_by_size
from ..processor import BaseNormaliser, compute_instructions
from .processor_property_checks import Snapshot, assert_instruction_safety


class _ExactIdentityNormaliser(BaseNormaliser):
    PRUNE_DOMINATED = False
    MULTIWAY = False


class _DominatedIdentityNormaliser(BaseNormaliser):
    PRUNE_DOMINATED = True
    MULTIWAY = False


class _MultiwayIdentityNormaliser(BaseNormaliser):
    PRUNE_DOMINATED = True
    MULTIWAY = True


_NORMALISERS: tuple[type[BaseNormaliser], ...] = (
    _ExactIdentityNormaliser,
    _DominatedIdentityNormaliser,
    _MultiwayIdentityNormaliser,
)


def _newline_terminated(lines: list[str]) -> bytes:
    return ''.join(f'{line}\n' for line in lines).encode()


_TEXT_LINE = st.text(
    alphabet=st.characters(codec='utf-8', exclude_characters=('\n', '\0')),
    max_size=12,
)
_TEXT_FILE = st.lists(_TEXT_LINE, min_size=1, max_size=5).map(_newline_terminated)
_TEXT_HISTORIES = st.lists(_TEXT_FILE, min_size=1, max_size=8)


@given(contents=_TEXT_HISTORIES)
@settings(max_examples=16, deadline=None, database=None)
def test_identity_normaliser_does_not_modify_inputs(*, contents: list[bytes]) -> None:
    """All pruning modes must treat identity-normalised source files as immutable inputs."""
    with TemporaryDirectory() as td:
        directory = Path(td)
        paths = [directory / f'{index:04}.txt' for index in range(len(contents))]
        for path, content in zip(paths, contents, strict=True):
            path.write_bytes(content)

        original_bytes = {path: path.read_bytes() for path in paths}
        original_mtimes = {path: path.stat().st_mtime_ns for path in paths}
        for normaliser_type in _NORMALISERS:
            # Fully consume the iterator because normalisation and grouping are lazy.
            list(compute_instructions(paths, Normaliser=normaliser_type, threads=None))

            # Identity normalisation may feed source paths to FileSet, but sorting and merging must only write temporaries.
            assert all(path.exists() for path in paths)
            assert {path: path.read_bytes() for path in paths} == original_bytes
            assert {path: path.stat().st_mtime_ns for path in paths} == original_mtimes


_HEADER = '# header'
_BOOM = '# simulated normalisation failure'


class _ProcessPoolNormaliser(BaseNormaliser):
    """Module-level normaliser so ProcessPoolExecutor can pickle it under spawn."""

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Path]:
        lines = path.read_text().splitlines()
        assert lines[0] == _HEADER
        items = lines[1:]
        if _BOOM in items:
            raise RuntimeError('simulated normalisation failure')
        normalised = self.tmp_dir / 'normalised'
        normalised.write_text(''.join(f'{item}\n' for item in items))
        yield normalised


class _ExactProcessPoolNormaliser(_ProcessPoolNormaliser):
    PRUNE_DOMINATED = False
    MULTIWAY = False


class _DominatedProcessPoolNormaliser(_ProcessPoolNormaliser):
    PRUNE_DOMINATED = True
    MULTIWAY = False


class _MultiwayProcessPoolNormaliser(_ProcessPoolNormaliser):
    PRUNE_DOMINATED = True
    MULTIWAY = True


_ERROR = RuntimeError('simulated normalisation failure')
_ATOMS = st.sampled_from(tuple('abcdef'))
_SNAPSHOT = st.frozensets(_ATOMS)
_SNAPSHOT_OR_ERROR = st.one_of(_SNAPSHOT, _SNAPSHOT, _SNAPSHOT, st.just(_ERROR))
_HISTORIES = st.lists(_SNAPSHOT_OR_ERROR, min_size=1, max_size=12)


def _materialise(*, directory: Path, snapshots: list[Snapshot]) -> list[Path]:
    paths: list[Path] = []
    for index, snapshot in enumerate(snapshots):
        # Reverse-lexical names ensure the output-order check is independent of filename sorting.
        path = directory / f'{len(snapshots) - index:04}.txt'
        items = [_BOOM] if isinstance(snapshot, Exception) else sorted(snapshot)
        path.write_text(''.join(f'{line}\n' for line in [_HEADER, *items]))
        paths.append(path)
    return paths


def test_three_workers_preserve_safety_across_two_chunk_boundaries() -> None:
    # Nine identical files divide into three chunks of three, each with one prunable interior.
    snapshots: list[Snapshot] = [frozenset({'a'})] * 9

    with TemporaryDirectory() as td:
        paths = _materialise(directory=Path(td), snapshots=snapshots)
        chunks = divide_by_size(buckets=3, paths=paths)
        assert [len(chunk) for chunk in chunks] == [3, 3, 3]

        instructions = list(compute_instructions(paths, Normaliser=_ExactProcessPoolNormaliser, threads=3))
        # Each chunk keeps its boundaries and prunes its middle; adjacent keeps expose both chunk boundaries.
        assert [type(instruction) for instruction in instructions] == [Keep, Prune, Keep] * 3
        assert_instruction_safety(snapshots=snapshots, paths=paths, instructions=instructions)


@pytest.mark.parametrize(
    ('normaliser_type', 'threads'),
    [
        pytest.param(_ExactProcessPoolNormaliser    , 1, id='exact-one-worker'),
        pytest.param(_ExactProcessPoolNormaliser    , 2, id='exact-two-workers'),
        pytest.param(_DominatedProcessPoolNormaliser, 1, id='dominated-one-worker'),
        pytest.param(_DominatedProcessPoolNormaliser, 2, id='dominated-two-workers'),
        pytest.param(_MultiwayProcessPoolNormaliser , 1, id='multiway-one-worker'),
        pytest.param(_MultiwayProcessPoolNormaliser , 2, id='multiway-two-workers'),
    ],
)  # fmt: skip
@given(snapshots=_HISTORIES)
@example(snapshots=[frozenset({'a'}), _ERROR, frozenset({'a'})])
@example(snapshots=[frozenset({'a'})] * 6)
@example(
    snapshots=[
        frozenset({'a', 'b'}),
        frozenset({'b', 'c'}),
        frozenset({'c', 'd'}),
        frozenset({'d'}),
    ]
)
@settings(max_examples=8, deadline=None, database=None)
def test_process_pool_preserves_safety_invariants(
    *,
    normaliser_type: type[BaseNormaliser],
    threads: int,
    snapshots: list[Snapshot],
) -> None:
    """Chunking may change decisions, but it must not weaken any pruning safety guarantee."""
    with TemporaryDirectory() as td:
        paths = _materialise(directory=Path(td), snapshots=snapshots)
        instructions = list(compute_instructions(paths, Normaliser=normaliser_type, threads=threads))

    assert_instruction_safety(snapshots=snapshots, paths=paths, instructions=instructions)
