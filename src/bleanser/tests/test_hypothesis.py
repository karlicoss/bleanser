from pathlib import Path

import pytest

from bleanser.modules.json_new import JsonNormaliser as Normaliser

from bleanser.tests.common import TESTDATA, actions, hack_attribute

data = TESTDATA / 'hypothesis'


# total time about 5s?
@pytest.mark.parametrize('num', range(10))
def test_normalise_one(tmp_path: Path, num) -> None:
    from bleanser.tests.common import skip_if_no_data; skip_if_no_data()
    path = data / 'hypothesis_20210625T220028Z.json'
    n = Normaliser()
    with n.do_cleanup(path, wdir=tmp_path):
        pass


# TODO less verbose mode for tests?
def test_all() -> None:
    from bleanser.tests.common import skip_if_no_data; skip_if_no_data()

    # todo share with main
    paths = list(sorted(data.glob('*.json')))
    assert len(paths) > 80, paths  # precondition

    # 4 workers: 64 seconds
    # 4 workers, pool for asdict: 42 seconds..
    # 2 workers: 81 seconds. hmmm
    with hack_attribute(Normaliser, 'DELETE_DOMINATED', True):
        res = actions(paths=paths, Normaliser=Normaliser, max_workers=4)
    remaining = {p.name for p in res.remaining}
    assert 0 < len(remaining) < len(paths), remaining  # sanity check

    assert {
        'hypothesis_2017-11-21.json',
        'hypothesis_2019-06-11.json',
        'hypothesis_2019-08-18.json',
        'hypothesis_20190923T003014Z.json',
        'hypothesis_20191216T123012Z.json',
        'hypothesis_20200325T140016Z.json',
        'hypothesis_20200720T140043Z.json',
        'hypothesis_20200828T123032Z.json',
        'hypothesis_20201012T140035Z.json',
        'hypothesis_20210223T213023Z.json',
        'hypothesis_20210625T220028Z.json',
    }.issubset(remaining), remaining
    # issubset because concurrency might end up in leaving more files than the absolute minimum

    assert len(remaining) < 30, remaining
# FIXME check move mode
