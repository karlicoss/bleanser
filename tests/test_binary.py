from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from typing import List

import pytest


TESTDATA = Path(__file__).absolute().parent / 'testdata'
data_dir = TESTDATA / 'instapaper'


@pytest.fixture(scope='module')
def data():
    # todo would be nice to have some read only view on it instead?
    src = data_dir.resolve()
    # TODO careful so it isn't filling up the disk...
    with TemporaryDirectory() as td:
        tdir = Path(td)
        shutil.copytree(src, tdir, dirs_exist_ok=True)
        yield tdir


from bleanser.core.processor import apply_instructions, compute_instructions
from bleanser.core.common import logger, Dry, Move, Remove, Mode
from bleanser.modules.binary import Normaliser


def via_fdupes(path: Path) -> List[str]:
    from subprocess import check_output
    lines = check_output(['fdupes', '-1', path]).decode('utf8').splitlines()
    to_delete = []
    for line in lines:
        to_delete.extend(line.split()[1:-1])
    return to_delete


def test_all(data: Path, tmp_path: Path) -> None:
    expected_deleted = {Path(p).name for p in via_fdupes(path=data)}

    paths = list(sorted(data.glob('*.json')))
    assert len(paths) > 20, paths  # precondition

    Normaliser.DIFF_FILTER = None # FIXME meh

    instructions = list(compute_instructions(paths, Normaliser=Normaliser, max_workers=0))
    apply_instructions(instructions, mode=Remove(), need_confirm=False)
    remaining = [x.name for x in sorted(data.iterdir())]

    deleted = {p.name for p in paths if p.name not in remaining}
    assert 0 < len(deleted) < len(paths), deleted # just in case

    assert deleted == expected_deleted
