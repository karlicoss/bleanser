from __future__ import annotations

import hashlib
from pathlib import Path

import more_itertools
import pytest

from ....tests.common import TESTDATA, actions, skip_if_no_data
from ..binary import BinaryNormaliser


def via_md5(path: Path) -> list[Path]:
    files = sorted(path.iterdir())

    def _file_hash(p: Path) -> str:
        return hashlib.md5(p.read_bytes()).hexdigest()

    grouped = more_itertools.map_reduce(files, keyfunc=_file_hash)

    to_delete = []
    for group in grouped.values():
        items = sorted(group)
        # keep the first and the last, prune the rest
        to_delete.extend(items[1:-1])
    return to_delete


# TODO maybe add some sanity checks?
# e.g. try guessing dates from filenames and making sure they are consistent with mtimes?
# todo need to resort removing to a single command
# and check 'remove' mode separately
@pytest.mark.parametrize(
    'data',
    [
        TESTDATA / 'instapaper',
        TESTDATA / 'hypothesis_xz',
    ],
)
def test_all(data: Path) -> None:
    skip_if_no_data()

    paths = sorted(data.glob('*.json*'))
    assert len(paths) > 20, paths  # precondition

    res = actions(paths=paths, Normaliser=BinaryNormaliser)

    expected_deleted = via_md5(path=data)
    assert res.pruned == expected_deleted


# FIXME hmm need to make sure --dry is the default (maybe add a cmdline test?)
