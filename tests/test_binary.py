from common import skip_if_not_karlicoss as pytestmark

from pathlib import Path
from typing import List

import pytest

from bleanser.modules.binary import Normaliser

from common import TESTDATA, actions

data = TESTDATA / 'instapaper'


def via_fdupes(path: Path) -> List[str]:
    from subprocess import check_output
    lines = check_output(['fdupes', '-1', path]).decode('utf8').splitlines()
    to_delete = []
    for line in lines:
        items = line.split()
        # meh... don't get why it's not processing them in order...
        items = list(sorted(items))
        to_delete.extend(items[1:-1])
    return list(sorted(to_delete))


# TODO maybe add some sanity checks?
# e.g. try guessing dates from filenames and making sure they are consistent with mtimes?
# todo need to resort removing to a single command
# and check 'remove' mode separately
def test_all() -> None:
    paths = list(sorted(data.glob('*.json')))
    # assert len(paths) > 20, paths  # precondition

    Normaliser.DIFF_FILTER = None
    # FIXME meh.. maybe instead instantiate an instance instead of class?

    res = actions(paths=paths, Normaliser=Normaliser)
    expected_deleted = [Path(p) for p in via_fdupes(path=data)]
    assert res.cleaned == expected_deleted

# FIXME hmm need to make sure --dry is the default (maybe add a cmdline test?)
