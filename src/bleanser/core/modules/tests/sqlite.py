from __future__ import annotations

import sqlite3
from pathlib import Path

from ...common import Keep, Prune
from ...processor import compute_groups, groups_to_instructions
from ..sqlite import SqliteNormaliser


def _make_db(out: Path, values: list[bytes], *, bad: bool = False) -> Path:
    with sqlite3.connect(out) as conn:
        conn.execute('CREATE TABLE `test` (bbb BLOB)')
        conn.executemany(
            'INSERT INTO `test` VALUES (?)',
            [(v,) for v in values],
        )
        if bad:
            # the only way I figured to actually force BLOB column to contain text values
            conn.execute('CREATE TABLE `bad` (bbb BLOB)')
            conn.execute('INSERT INTO `bad` SELECT cast(bbb AS TEXT) FROM `test`')
            conn.execute('DROP TABLE `test`')
            conn.execute('ALTER TABLE `bad` RENAME TO `test`')
    conn.close()
    return out


def test_sqlite_blobs_good(tmp_path: Path) -> None:
    """
    In this case we have blob data in BLOB column -- so cleanup should work as expected
    """

    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = False
        PRUNE_DOMINATED = True

    db0 = _make_db(tmp_path / '0.db', [b'\x00\x01'])
    db1 = _make_db(tmp_path / '1.db', [b'\x00\x01', b'\x01\x02'])
    db2 = _make_db(tmp_path / '2.db', [b'\x00\x01', b'\x01\x02', b'\x02\x03'])
    db3 = _make_db(tmp_path / '3.db', [b'\x00\x01', b'\x01\x02', b'\x02\x03', b'\x03\x04'])
    dbs = [db0, db1, db2, db3]

    groups = list(compute_groups(dbs, Normaliser=TestNormaliser))
    instructions = list(groups_to_instructions(groups))

    assert [type(i) for i in instructions] == [
        Keep,
        Prune,
        Prune,
        Keep,
    ]


def test_sqlite_blobs_bad(tmp_path: Path) -> None:
    """
    In this case we have text (!) data in BLOB column.
    This will cause errors during cleanup so we'll keep all inputs (even though dbs are identical here)
    """

    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = False
        PRUNE_DOMINATED = True

    db0 = _make_db(tmp_path / '0.db', [b'\x00', b'\x01', b'\x02'], bad=True)
    db1 = _make_db(tmp_path / '1.db', [b'\x00', b'\x01', b'\x02'], bad=True)
    db2 = _make_db(tmp_path / '2.db', [b'\x00', b'\x01', b'\x02'], bad=True)
    db3 = _make_db(tmp_path / '3.db', [b'\x00', b'\x01', b'\x02'], bad=True)
    dbs = [db0, db1, db2, db3]

    groups = list(compute_groups(dbs, Normaliser=TestNormaliser))
    instructions = list(groups_to_instructions(groups))

    assert [type(i) for i in instructions] == [
        Keep,
        Keep,
        Keep,
        Keep,
    ]


def test_sqlite_blobs_allowed(tmp_path: Path) -> None:
    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = False
        PRUNE_DOMINATED = True

        ALLOWED_BLOBS = frozenset({('test', 'bbb')})

    db0 = _make_db(tmp_path / '0.db', [b'\x00\x01'], bad=True)
    db1 = _make_db(tmp_path / '1.db', [b'\x00\x02'], bad=True)
    db2 = _make_db(tmp_path / '2.db', [b'\x00\x03'], bad=True)
    db3 = _make_db(tmp_path / '3.db', [b'\x00\x04'], bad=True)
    dbs = [db0, db1, db2, db3]

    groups = list(compute_groups(dbs, Normaliser=TestNormaliser))
    instructions = list(groups_to_instructions(groups))

    # this kinda demonstrates what happens if we're not careful and mess up ALLOWED_BLOBS
    # sqlite3 will end up dumping supposedly blob data as empty strings
    # and this will clean up files that shouldn't be cleaned up (in the files above all data is different!)
    assert [type(i) for i in instructions] == [
        Keep,
        Prune,
        Prune,
        Keep,
    ]
