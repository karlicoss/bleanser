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


    for r in {
            'reddit_20220101T192056Z.json',  # subreddit description changed
            'reddit_20220101T193109Z.json',  # also subreddit description
            'reddit_20220102T142057Z.json',  # kept author changed (likely deleted?)
    }:
        assert r in {p.name for p in res.remaining}


    assert res.remaining == [
        paths[0 ], # first one
        paths[2 ], # last in group
        paths[3 ], # subreddit descr change
        paths[4 ], # subreddit descr change

        paths[7 ], # first in group
        paths[8 ], # author changed
        paths[11], # last in group
    ]
