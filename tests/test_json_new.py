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

    res = actions(paths=paths, Normaliser=Normaliser)

    # /data/exports/reddit/reddit_20211230T034059Z.json.xz /data/exports/reddit/reddit_20211230T035056Z.json.xz
    assert [p.name for p in res.remaining] == [
        'reddit_20211230T034059Z.json',  # ??
        'reddit_20211230T035056Z.json',  # some things legit disappeared due to api limits

        'reddit_20211230T041057Z.json',  # ??
        'reddit_20220101T185059Z.json',  # subreddit description

        'reddit_20220101T191057Z.json',  # ??
        'reddit_20220101T192056Z.json',  # subreddit description changed
        'reddit_20220101T193109Z.json',  # also subreddit description

        'reddit_20220102T132059Z.json',  # ??
        'reddit_20220102T142057Z.json',  # author changed (likely deleted?)
        'reddit_20220102T164059Z.json',  # last in group
    ]
