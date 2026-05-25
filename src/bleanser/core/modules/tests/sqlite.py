from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from ...common import Keep, Prune
from ...processor import compute_groups, compute_instructions, groups_to_instructions
from ..sqlite import SqliteNormaliser, _postprocess_dump_hex, _postprocess_dump_hex_line


def _dict2db(d: dict, *, to: Path) -> Path:
    with sqlite3.connect(to) as conn:
        for table_name, rows in d.items():
            schema = rows[0]
            s = ', '.join(schema)
            qq = ', '.join('?' for _ in schema)
            conn.execute(f'CREATE TABLE `{table_name}` ({s})')
            conn.executemany(f'INSERT INTO `{table_name}` VALUES ({qq})', rows[1:])
    conn.close()
    return to  # just for convenience


def test_sqlite_simple(tmp_path: Path) -> None:
    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = False
        PRUNE_DOMINATED = True

    func = lambda paths: compute_groups(paths, Normaliser=TestNormaliser)

    d: dict[str, Any] = {'tq': [['col1', 'col2']]}
    ### just one file
    db1 = _dict2db(d, to=tmp_path / '1.db')
    [g11] = func([db1])
    assert g11.items  == [db1]  # fmt: skip
    assert g11.pivots == [db1]
    ###

    ### simple 'dominates' test
    d['t1'] = [
        ['col1', 'col2'],
        [1     , 2     ],
        [3     , 4     ],
    ]  # fmt: skip
    db2 = _dict2db(d, to=tmp_path / '2.db')

    [g21] = func([db1, db2])
    assert g21.items  == [db1, db2]  # fmt: skip
    assert g21.pivots == [db1, db2]
    ###

    ### test error handling
    db3 = tmp_path / '3.db'
    db3.write_text('BAD')

    # TODO wow it's really messy...
    # fmt: off
    [x1, x2] = func([db3, db1, db2])
    assert x1.items  == [db3]
    assert x1.pivots == [db3]
    assert x2.items  == [db1, db2]
    assert x2.pivots == [db1, db2]

    [g31, g32, g33] = func([db1, db2, db3])
    assert g31 == g21
    assert g32.items  == [db2]
    assert g32.pivots == [db2]
    assert g33.items  == [db3]
    assert g33.pivots == [db3]
    # fmt: on
    # FIXME check error reason
    ###

    ### test 'same' handling
    db4 = _dict2db(d, to=tmp_path / '4.db')
    db5 = _dict2db(d, to=tmp_path / '5.db')
    db6 = _dict2db(d, to=tmp_path / '6.db')

    dbs = [db1, db2, db3, db4, db5, db6]
    [_g41, _g42, g43, g44] = func(dbs)
    assert g43 == g33
    assert g44.items  == [db4, db5, db6]  # fmt: skip
    assert g44.pivots == [db4,      db6]  # fmt: skip
    ###

    instrs = compute_instructions(
        dbs,
        Normaliser=SqliteNormaliser,
        threads=None,
    )
    assert list(map(type, instrs)) == [
        Keep,   # 1
        Keep,   # 2
        Keep,   # 3
        Keep,   # 4, keep the boundary
        Prune,  # 5
        Keep,   # 6, keep the boundary
    ]  # fmt: skip

    ### test when stuff was removed
    del d['t1'][-1]
    db7 = _dict2db(d, to=tmp_path / '7.db')
    dbs = [db1, db2, db3, db4, db5, db6, db7]
    [_, _, _, g54, g55, g56] = func(dbs)
    assert g54 == g44
    # TODO ugh. this is confusing... why it emits more pivots?
    assert g55.items  == [db6]  # fmt: skip
    assert g55.pivots == [db6]
    assert g56.items  == [db7]  # fmt: skip
    assert g56.pivots == [db7]
    ###

    instrs = compute_instructions(
        dbs,
        Normaliser=SqliteNormaliser,
        threads=None,
    )
    assert list(map(type, instrs)) == [
        Keep,   # 1
        Keep,   # 2
        Keep,   # 3
        Keep,   # 4, keep the boundary
        Prune,  # 5
        Keep,   # 6, keep the boundary
        Keep,   # 7,
    ]  # fmt: skip


def _make_long_dump_db(to: Path, *, ignored_value: int, payload_suffix: str = '') -> Path:
    with sqlite3.connect(to) as conn:
        conn.execute('CREATE TABLE payloads (id INTEGER, body TEXT)')
        conn.execute('CREATE TABLE ignored (value INTEGER)')
        conn.execute('INSERT INTO ignored VALUES (?)', (ignored_value,))
        # Regression shape for uutils sort 0.8.0: a few long lines made only of "a" are enough for `sort --unique --merge` to split SQL dump lines.
        body = 'a' * 32_000 + payload_suffix
        for i in range(3):
            conn.execute('INSERT INTO payloads VALUES (?, ?)', (i, body))

    return to


def test_sqlite_long_dump_lines_end_to_end(tmp_path: Path) -> None:
    class TestNormaliser(SqliteNormaliser):
        def cleanup(self, c: sqlite3.Connection) -> None:
            c.execute('DROP TABLE ignored')

    db0 = _make_long_dump_db(tmp_path / '0.db', ignored_value=0)
    db1 = _make_long_dump_db(tmp_path / '1.db', ignored_value=1)
    db2 = _make_long_dump_db(tmp_path / '2.db', ignored_value=2)
    db3 = _make_long_dump_db(tmp_path / '3.db', ignored_value=3, payload_suffix='b')

    instructions = list(compute_instructions([db0, db1, db2, db3], Normaliser=TestNormaliser, threads=None))

    assert [type(i) for i in instructions] == [
        Keep,
        Prune,
        Keep,
        Keep,
    ]


@pytest.mark.parametrize(
    ('line', 'expected'),
    [
        (b"INSERT INTO test VALUES(1, 'plain');\n", b"INSERT INTO test VALUES(1, 'plain');\n"),
        (b"INSERT INTO test VALUES(X'ffd8');\n", b"INSERT INTO test VALUES(X'ffd8');\n"),
        (b"INSERT INTO test VALUES(X'7b7d');\n", b"INSERT INTO test VALUES(X'{}');\n"),
        (b"INSERT INTO test VALUES(X'7B7D');\n", b"INSERT INTO test VALUES(X'{}');\n"),
        (b"INSERT INTO test VALUES(X'5b5d');\n", b"INSERT INTO test VALUES(X'5b5d');\n"),
        (b"INSERT INTO test VALUES(X'7b2261223a317d');\n", b"INSERT INTO test VALUES(X'{\"a\":1}');\n"),
        (b"INSERT INTO test VALUES(X'7B2261223A317D');\n", b"INSERT INTO test VALUES(X'{\"a\":1}');\n"),
        (b"INSERT INTO test VALUES(X'7b0a7d');\n", b"INSERT INTO test VALUES(X'{<NEWLINE>}');\n"),
        (
            b"INSERT INTO test VALUES(X'7b7d', X'5b5d', X'7b0d0a7d');\n",
            b"INSERT INTO test VALUES(X'{}', X'5b5d', X'{<NEWLINE>}');\n",
        ),
    ],
)
def test_sqlite_hex_postprocess_line(line: bytes, expected: bytes) -> None:
    assert _postprocess_dump_hex_line(line) == expected


def test_sqlite_hex_postprocess_file(tmp_path: Path) -> None:
    src = tmp_path / 'dump.sql'
    dst = tmp_path / 'dump_nohex.sql'

    src.write_bytes(b"INSERT INTO test VALUES(1, 'plain');\n")
    assert _postprocess_dump_hex(src=src, dst=dst) == src
    assert src.read_bytes() == b"INSERT INTO test VALUES(1, 'plain');\n"
    assert not dst.exists()

    src.write_bytes(b"INSERT INTO test VALUES(X'5b5d');\n")
    assert _postprocess_dump_hex(src=src, dst=dst) == src
    assert src.read_bytes() == b"INSERT INTO test VALUES(X'5b5d');\n"
    assert not dst.exists()

    src.write_bytes(b"INSERT INTO a VALUES(1);\nINSERT INTO test VALUES(X'7b7d');\n")
    assert _postprocess_dump_hex(src=src, dst=dst) == src
    assert src.read_bytes() == b"INSERT INTO a VALUES(1);\nINSERT INTO test VALUES(X'{}');\n"
    assert not dst.exists()


@pytest.mark.parametrize('multiway', [False, True])
def test_sqlite_many(*, tmp_path: Path, multiway: bool) -> None:
    class TestNormaliser(SqliteNormaliser):
        MULTIWAY = multiway
        PRUNE_DOMINATED = True

    N = 2000

    paths = []
    d: dict[str, Any] = {}
    for i in range(N):
        if i % 10 == 0:
            # flush so sometimes it emits groups
            d = {'t': [('number',)]}
        d['t'].append((i,))  # ty: ignore[invalid-argument-type]
        p = _dict2db(d, to=tmp_path / f'{i:04}.db')
        paths.append(p)

    # shouldn't crash
    _instructions = list(
        compute_instructions(
            paths,
            Normaliser=TestNormaliser,
            threads=None,
        )
    )


# TODO add some tests for my own dbs? e.g. stashed


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
