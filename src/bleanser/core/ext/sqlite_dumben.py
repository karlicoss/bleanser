#!/usr/bin/env python3
# A tool to 'dumb down' an sqlite database and convert into just data rows
# Basically it strips all
# - views
# - indices
# - triggers
# - contstraints
# this is useful if you want to mess/cleanup the database, but don't want to trip over constraints/triggers
# NOTE: handling everything as bytes since not sure I wanna mess with encoding here (esp. row data encoding)

import contextlib
from functools import cached_property
from pathlib import Path
import re
import sqlite3
from subprocess import check_output, Popen, PIPE
import sys
from typing import Iterator, List, Dict, Optional, IO


class _Filter:
    def __init__(self, db: Path) -> None:
        # TODO check that it's a db?
        self.db = db

    @cached_property
    def tables(self) -> Dict[str, Dict[str, str]]:
        res: Dict[str, Dict[str, str]] = {}
        with sqlite3.connect(f'file:{self.db}?immutable=1', uri=True) as conn:
            tables = []
            for row in conn.execute('SELECT name, type FROM sqlite_master'):
                (table, type_) = row
                if type_ in {'index', 'view', 'trigger'}:
                    # todo log what kind of things we are filtering out?
                    continue
                assert type_ == 'table', (table, type_)
                tables.append(table)

            for table in tables:
                schema: Dict[str, str] = {}
                for row in conn.execute(f'PRAGMA table_info({table})'):
                    col   = row[1]
                    type_ = row[2]
                    schema[col] = type_
                res[table] = schema
        return res

    def _filter_line(self, sql_line: bytes) -> Optional[bytes]:
        line = sql_line.strip()  # sometimes might have empty lines etc

        allowed = [
            b'INSERT .*INTO ',
            b'DELETE FROM ',
            b'PRAGMA',
            b'BEGIN TRANSACTION',
            b'COMMIT',
        ]
        if any(re.match(s, line) for s in allowed):
            # meh
            if line.startswith(b'DELETE FROM sqlite_sequence') or line.startswith(b'INSERT INTO sqlite_sequence'):
                # smth to do with autoincrement
                return b''
            if line.startswith(b'DELETE FROM sqlite_stat') or line.startswith(b'INSERT INTO sqlite_stat'):
                # smth to do with ANALYZE
                return b''

            if re.match(b'INSERT .*INTO.*FROM ', line):
                # meh... prob don't need dynamic statements like this, annoying if they insert from views
                return b''

            return line

        if line.startswith(b'CREATE TABLE '):
            si = line.find(b'(')
            assert si != -1, line
            # backticks are quotes (e.g. if table name is a special keyword...)
            table_name = line[len(b'CREATE TABLE '): si]
            table_name = table_name.replace(b'IF NOT EXISTS ', b'')
            table_name = table_name.lstrip()  # sometimes happens
            table_name = table_name.strip().strip(b"`'\"")

            schema = self.tables[table_name.decode('utf8')]
            # always escape in case the column is named 'index' or someting
            line2 = line[:si] + b' (' + b', '.join(f'`{k}` {v}'.encode('utf8') for k, v in schema.items()) + b');\n'
            return line2

        dropped = [
            b'CREATE INDEX ', b'CREATE UNIQUE INDEX ',
            b'CREATE VIEW ',
            b'CREATE TRIGGER ',

            b'ANALYZE ',  # some optimization thing https://www.sqlite.org/lang_analyze.html
        ]
        if any(line.startswith(s) for s in dropped):
            return b''

        raise RuntimeError(line)


    def _filtered(self, sql_lines: Iterator[bytes]) -> Iterator[bytes]:
        for line in sql_lines:
            res = self._filter_line(line)
            if res is not None:
                yield res


    def as_sql_lines(self) -> Iterator[bytes]:
        with Popen(['sqlite3', '-readonly', f'file://{self.db}?immutable=1', '.dump'], stdout=PIPE) as p:
            out = p.stdout; assert out is not None
            lines = _as_sql_lines(iter(out))
            yield from self._filtered(lines)


def _as_sql_lines(sql_dump: Iterator[bytes]) -> Iterator[bytes]:
    """
    Kinda annoying, for some reason sql dump breaks line at table/view creation statements...
    """
    g: List[bytes] = []

    def dump() -> bytes:
        nonlocal g
        res = b''.join(g)
        g = []
        return res

    inside_begin = False
    for line in sql_dump:
        assert line.endswith(b'\n'), line

        is_begin = line.startswith(b'BEGIN\n')
        is_end = line.endswith(b'END;\n')

        if is_begin:
            assert not inside_begin
            inside_begin = True

        if is_end:
            assert inside_begin
            inside_begin = False

        should_emit = (inside_begin and is_end) or (not inside_begin and line.endswith(b';\n'))
        if should_emit:
            g.append(line)
            yield dump()
        else:
            g.append(line[:-1] + b' ')

    if len(g) > 0:
        yield dump()
    assert not inside_begin


def run(*, db: Path, output: Optional[Path], output_as_db: bool) -> None:
    if output is not None:
        assert not output.exists(), output

    if output is None:
        assert output_as_db is False

    popen: Optional[Popen] = None
    ctx_stack = contextlib.ExitStack()
    out: IO[bytes]
    if output is None:
        out = sys.stdout.buffer
    else:
        if output_as_db:
            popen = ctx_stack.enter_context(Popen(['sqlite3', '-bail', output], stdin=PIPE))
            pin = popen.stdin; assert pin is not None
            out = pin
        else:
            out = ctx_stack.enter_context(output.open('wb'))

    with ctx_stack:
        for line in _Filter(db=db).as_sql_lines():
            out.write(line)

    if popen is not None:
        popen.wait()
        ret = popen.returncode
        assert ret == 0, ret


def main() -> None:
    from argparse import ArgumentParser
    p = ArgumentParser()
    p.add_argument('--output-as-db', action='store_true')
    p.add_argument('--output', type=Path, required=False)
    p.add_argument('db', type=Path)
    args = p.parse_args()

    run(db=args.db, output=args.output, output_as_db=args.output_as_db)


if __name__ == '__main__':
    main()

# FIXME add some tests, maybe some dbs from testdata with triggers/constraints
# e.g. KoboReader-20211130.sqlite seems to have
# CREATE TRIGGER kobo_plus_asset_cleanup
#
# TODO fb messenger is a good db to test on... lots of weird shit, e.g. transactions
