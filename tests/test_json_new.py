from common import skip_if_not_karlicoss as pytestmark

from pathlib import Path
from typing import List

import pytest

from bleanser.modules.json_new import Normaliser

from common import TESTDATA, actions


@pytest.mark.parametrize('data', [
    TESTDATA / 'reddit',
])
def test_all(data: Path) -> None:
    paths = list(sorted(data.glob('*.json*')))
    # assert len(paths) > 20, paths  # precondition

    res = actions(paths=paths, Normaliser=Normaliser)
    remaining = [paths[0], paths[1], paths[4]]

    assert res.remaining == remaining
