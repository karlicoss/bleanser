from common import skip_if_not_karlicoss as pytestmark

from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import pytest


TESTDATA = Path(__file__).absolute().parent / 'testdata'
data_dir = TESTDATA / 'hypothesis'

@pytest.fixture
def data():
    src = data_dir.resolve()
    # TODO careful so it isn't filling up the disk...
    with TemporaryDirectory() as td:
        tdir = Path(td)
        shutil.copytree(src, tdir, dirs_exist_ok=True)
        yield tdir


from bleanser.core.processor import apply_instructions, compute_instructions
from bleanser.core.common import logger, Dry, Move, Remove, Mode
from bleanser.core.json import JsonNormaliser as Normaliser


# TODO less verbose mode for tests?
def test(data: Path, tmp_path: Path) -> None:
    # FIXME share with main
    paths = list(sorted(data.glob('*.json')))
    assert len(paths) > 80, paths  # precondition

    # 4 workers: 64 seconds
    # 2 workers: 81 seconds. hmmm
    instructions = list(compute_instructions(paths, Normaliser=Normaliser, max_workers=2)) # , max_workers=0
    apply_instructions(instructions, mode=Remove())
    remaining = [x.name for x in sorted(data.iterdir())]
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
    # issubset because concurrency might leave more files than the absolute minimum
    assert len(remaining) < 30, remaining
# FIXME check move mode
