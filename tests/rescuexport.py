from common import skip_if_not_karlicoss as pytestmark

from pathlib import Path
import shutil
from tempfile import TemporaryDirectory

import pytest


@pytest.fixture
def data():
    src = Path('/tmp/rescuetime').resolve()
    # TODO careful so it isn't filling up the disk...
    with TemporaryDirectory() as td:
        tdir = Path(td)
        shutil.copytree(src, tdir, dirs_exist_ok=True)
        yield tdir


from bleanser.core.processor import apply_instructions, compute_instructions
from bleanser.core.common import logger, Dry, Move, Remove, Mode
from bleanser.core.common import Keep, Delete
from bleanser.core.json import JsonNormaliser as Normaliser


def test(data: Path, tmp_path: Path) -> None:
    # FIXME share with main
    paths = list(sorted(data.glob('*.json')))
    instructions = list(compute_instructions(paths, Normaliser=Normaliser, max_workers=0))
    [k1, k2, k3] = instructions
    assert isinstance(k1, Keep), k1
    assert k2.path.name == 'rescuetime_20200219T010207Z.json'
    assert isinstance(k2, Keep), k2
    assert isinstance(k3, Keep), k3
    # FIXME check move mode
    apply_instructions(instructions, mode=Dry())
    # TODO these should be present in the result... not sure how to properly test?
    # maybe just grep... after applying instructions
    # Entry(dt=datetime.datetime(2020, 2, 19, 0, 55), duration_s=9, activity='mobile - com.android.launcher3'),
    # Entry(dt=datetime.datetime(2020, 2, 19, 0, 55), duration_s=9, activity='mobile - com.termux'),
