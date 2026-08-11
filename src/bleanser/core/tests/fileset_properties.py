"""Property-based checks for FileSet's line-set algebra."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory

from hypothesis import example, given, settings
from hypothesis import strategies as st

from ..processor import FileSet

# Keep generated text safe for line-oriented Unix tools while covering blank lines, whitespace, and valid Unicode.
_SAFE_CHARACTERS = (' ', '\t', '0', 'A', 'a', 'z', 'é', 'λ', '界', '🙂')
_LINE = st.text(alphabet=st.sampled_from(_SAFE_CHARACTERS), min_size=0, max_size=8)
_RAW_FILE = st.lists(_LINE, min_size=0, max_size=16)
_RAW_FILES = st.lists(_RAW_FILE, min_size=0, max_size=8)


def _write_files(*, directory: Path, prefix: str, files: list[list[str]]) -> list[Path]:
    paths = []
    for index, lines in enumerate(files):
        path = directory / f'{prefix}-{index}'
        # Every generated value is one physical line, including the empty string, and every line ends with a newline.
        path.write_bytes(''.join(f'{line}\n' for line in lines).encode())
        paths.append(path)
    return paths


def _line_set(files: list[list[str]]) -> set[str]:
    return {line for lines in files for line in lines}


def _fileset_lines(fileset: FileSet) -> set[str]:
    return set(fileset.merged.read_text(encoding='utf-8').splitlines())


@given(source_files=_RAW_FILES, target_files=_RAW_FILES, added_files=_RAW_FILES)
@example(
    source_files=[['λ', '', ' ', 'λ', 'a']],
    target_files=[['a', '\t', '界'], ['', 'é']],
    added_files=[['🙂', '🙂', ' '], ['界', 'λ']],
)
@settings(max_examples=100, deadline=None, database=None)
def test_fileset_matches_set_algebra(
    *,
    source_files: list[list[str]],
    target_files: list[list[str]],
    added_files: list[list[str]],
) -> None:
    # NOTE: can't use tmp_path fixture since hypothesis reuses them across samples
    with TemporaryDirectory() as td:
        root = Path(td)
        inputs = root / 'inputs'
        inputs.mkdir()
        work = root / 'work'
        work.mkdir()

        # Source and target are independent operands for equality and subset checks.
        # Added files extend source for the direct-versus-incremental union checks.
        source_paths = _write_files(directory=inputs, prefix='source', files=source_files)
        target_paths = _write_files(directory=inputs, prefix='target', files=target_files)
        added_paths = _write_files(directory=inputs, prefix='added', files=added_files)
        all_paths = [*source_paths, *target_paths, *added_paths]
        input_bytes = {path: path.read_bytes() for path in all_paths}
        input_mtimes = {path: path.stat().st_mtime_ns for path in all_paths}

        source_lines = _line_set(source_files)
        target_lines = _line_set(target_files)
        expected_union = source_lines | _line_set(added_files)

        with ExitStack() as stack:
            source = stack.enter_context(FileSet(source_paths, wdir=work))
            target = stack.enter_context(FileSet(target_paths, wdir=work))

            # Construction must canonicalise each raw collection to its Python set of physical lines.
            assert _fileset_lines(source) == source_lines
            assert _fileset_lines(target) == target_lines

            # FileSet equality and containment must have exactly the same semantics as Python sets of physical lines.
            assert source.issame  (target) == (source_lines == target_lines)  # fmt: skip
            assert source.issubset(target) == (source_lines <= target_lines)
            assert target.issubset(source) == (target_lines <= source_lines)

            source_items = list(source.items)
            source_merged_bytes = source.merged.read_bytes()

            # Adding every file at once or one at a time must produce the Python union regardless of input order or duplicates.
            direct = stack.enter_context(source.union(*added_paths))
            incremental = source
            for added_path in added_paths:
                incremental = stack.enter_context(incremental.union(added_path))

            assert _fileset_lines(direct) == expected_union
            assert _fileset_lines(incremental) == expected_union
            assert direct.issame(incremental)

            # union returns a new FileSet without changing the source's inputs or canonical merged representation.
            assert source.items == source_items
            assert source.merged.read_bytes() == source_merged_bytes
            assert _fileset_lines(source) == source_lines

        # FileSet may only write its private merged files; inputs must remain byte-for-byte identical.
        assert {path: path.read_bytes() for path in all_paths} == input_bytes
        assert {path: path.stat().st_mtime_ns for path in all_paths} == input_mtimes
