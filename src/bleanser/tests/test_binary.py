from __future__ import annotations

from pathlib import Path

import pytest

from bleanser.modules.binary import Normaliser
from bleanser.tests.common import TESTDATA, actions, hack_attribute, skip_if_no_data

# TODO ugh. how to make relative imports work? pytest doesn't like them...


def via_fdupes(path: Path) -> list[str]:
    from subprocess import check_output

    lines = check_output(['fdupes', '-1', path]).decode('utf8').splitlines()
    to_delete = []
    for line in lines:
        items = line.split()
        # meh... don't get why it's not processing them in order...
        items = sorted(items)
        to_delete.extend(items[1:-1])
    return sorted(to_delete)


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

    with hack_attribute(Normaliser, '_DIFF_FILTER', None):
        res = actions(paths=paths, Normaliser=Normaliser)

    expected_deleted = [Path(p) for p in via_fdupes(path=data)]
    assert res.pruned == expected_deleted


# FIXME hmm need to make sure --dry is the default (maybe add a cmdline test?)
