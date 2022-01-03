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
    # TODO add a test for multiway
    paths = list(sorted(data.glob('*.json*')))

    res = actions(paths=paths, Normaliser=Normaliser)

    # /data/exports/reddit/reddit_20211230T034059Z.json.xz /data/exports/reddit/reddit_20211230T035056Z.json.xz
    assert [p.name for p in res.remaining] == [
        'reddit_20211227T164130Z.json',  # first in group
        'reddit_20211227T170106Z.json',  # saved item rolled over
        'reddit_20211227T171058Z.json',  # some saved items rolled over

        'reddit_20211227T173058Z.json',  # keeping boundary
        'reddit_20211230T034059Z.json',  # some items rolled over
        'reddit_20211230T035056Z.json',  # some things legit disappeared due to api limits

        'reddit_20211230T041057Z.json',  # keeping boundary for the next one
        'reddit_20220101T185059Z.json',  # subreddit description

        'reddit_20220101T191057Z.json',  # ??
        'reddit_20220101T192056Z.json',  # subreddit description changed
        'reddit_20220101T193109Z.json',  # also subreddit description

        'reddit_20220102T132059Z.json',  # ??
        'reddit_20220102T142057Z.json',  # author changed (likely deleted?)
        'reddit_20220102T164059Z.json',  # last in group
    ]
