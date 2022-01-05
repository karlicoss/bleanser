from common import skip_if_not_karlicoss as pytestmark

from pathlib import Path

import pytest

from bleanser.modules.json_new import Normaliser

from bleanser.tests.common import TESTDATA, actions, hack_attribute


def test_rescuetime() -> None:
    data = TESTDATA / 'rescuetime'
    paths = list(sorted(data.glob('*.json*')))

    with hack_attribute(Normaliser, 'MULTIWAY', True), hack_attribute(Normaliser, 'DELETE_DOMINATED', True):
        res = actions(paths=paths, Normaliser=Normaliser)

    assert [p.name for p in res.remaining] == [
        'rescuetime_2018-01-02.json.xz',
        'rescuetime_2018-01-04.json.xz',
        'rescuetime_2018-01-07.json.xz',
        'rescuetime_2018-01-10.json.xz',
        'rescuetime_2018-01-11.json.xz',

        # todo these should be present in the result for the following group
        # not sure how to properly test?
        # maybe just grep... after applying instructions
        # Entry(dt=datetime.datetime(2020, 2, 19, 0, 55), duration_s=9, activity='mobile - com.android.launcher3'),
        # Entry(dt=datetime.datetime(2020, 2, 19, 0, 55), duration_s=9, activity='mobile - com.termux'),
        'rescuetime_20200204T010205Z.json',
        'rescuetime_20200219T010207Z.json',
        'rescuetime_20200305T010206Z.json',

        'rescuetime_20211209T011109Z.json.xz',
        'rescuetime_20211218T011116Z.json.xz',
        'rescuetime_20211220T011110Z.json.xz',
        'rescuetime_20211224T011109Z.json.xz',
    ]
