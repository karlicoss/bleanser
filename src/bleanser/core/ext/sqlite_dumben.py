#!/usr/bin/env python3
# A tool to 'dumb down' an sqlite database and convert into just data rows
# Basically it strips all
# - views
# - indices
# - triggers
# - constraints
# this is useful if you want to mess/cleanup the database, but don't want to trip over constraints/triggers
# NOTE: handling everything as bytes since not sure I wanna mess with encoding here (esp. row data encoding)
from __future__ import annotations

import hashlib
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path
from subprocess import DEVNULL, check_call, check_output
from tempfile import TemporaryDirectory

Tables = dict[str, dict[str, str]]


def _get_tables(db: Path) -> Tables:
    res: Tables = {}
    with sqlite3.connect(f'file:{db}?immutable=1', uri=True) as conn:
        tables = []
        for row in conn.execute('SELECT name, type FROM sqlite_master'):
            (table, type_) = row
            if type_ in {'index', 'view', 'trigger'}:
                # todo log what kind of things we are filtering out?
                continue
            assert type_ == 'table', (table, type_)
            tables.append(table)

        for table in tables:
            schema: dict[str, str] = {}
            for row in conn.execute(f'PRAGMA table_info({table})'):
                col = row[1]
                type_ = row[2]
                schema[col] = type_
            res[table] = schema
    return res


def _sqlite(*cmd):
    return ['sqlite3', '-bail', *cmd]


def _dumben_db(output_db: Path) -> None:
    # expected to operate on output_db directly
    assert output_db.exists(), output_db

    # hmm. CREATE TABLE syntax seems ridiculously complicated https://www.sqlite.org/lang_createtable.html
    # so seems pretty hopeless to sanitize off the constraints purely via sqlite?
    # the only easy win is making it single line
    # "UPDATE sqlite_master SET sql = replace(sql, char(10), ' ');"

    allow_writable_schema = [
        # seems like some versions of sqlite (e.g. on osx don't allow writable schema without this pragma)
        # https://github.com/tekartik/sqflite/blob/master/sqflite_common_ffi/doc/custom_pragmas.md?plain=1
        "PRAGMA sqflite -- db_config_defensive_off",
        "PRAGMA writable_schema=ON",
    ]

    # first delete virtual tables -- they might render it impossible to do anything with database at all due to USING
    # e.g. fb messenger android msys database has this CREATE VIRTUAL TABLE msys_experiment_cache USING experiment_cache
    # either way virtual tables are basically views, no need to keep them
    with sqlite3.connect(output_db) as conn:
        for cmd in allow_writable_schema:
            conn.execute(cmd)
        conn.execute('DELETE FROM sqlite_master WHERE sql LIKE "%CREATE VIRTUAL TABLE%"')
    conn.close()

    tables = _get_tables(output_db)

    updates = []
    for name, schema in tables.items():
        simple_create = f'CREATE TABLE `{name}` (' + ', '.join(f'`{k}` {v}' for k, v in schema.items()) + ')'
        # TODO dunno if worth keeping autoincrement
        # without it, all columns with numerical id end up as NULL. although maybe for the best?
        upd = f'UPDATE sqlite_master SET sql = "{simple_create}" WHERE name = "{name}";'
        updates.append(upd)

    cmds = [
        *allow_writable_schema,
        # drop table doesn't work for special sqlite_ tables
        # sqlite_sequence is something to do with autoincrement, ends up with some indices noise otherwise
        # sqlite_stat{1,2,3,4} is something to do with ANALYZE query
        'DELETE FROM sqlite_master WHERE name = "sqlite_sequence" OR name LIKE "sqlite_stat%";',
        #
        'DELETE FROM sqlite_master WHERE type IN ("view", "trigger", "index");',
        *updates,
        #
        # without vacuum, sometimes ended up with "rootpage disagrees with header error", from sqlite code seemed like it had something to do with autovacuum
        'VACUUM',
    ]

    # need to set isolation level to None, otherwise VACUUM fails
    with sqlite3.connect(output_db, isolation_level=None) as conn:
        for cmd in cmds:
            conn.execute(cmd)
    conn.close()

    # make sure it's not corrupted
    # redirect output to DEVNULL, otherwise it's printing "ok" which is a bit annoying
    subprocess.check_call(_sqlite(output_db, 'PRAGMA integrity_check;'), stdout=DEVNULL)


def run(*, db: Path, output: Path | None, output_as_db: bool) -> None:
    if output is not None:
        assert not output.exists(), output

    if output is None:
        assert output_as_db is False, "can't output to stdout as a binary database"

    if output_as_db:
        assert output is not None

        dumben_cache: Path | None = None
        _DUMBEN_CACHE_BASE = os.environ.get('SQLITE_DUMBEN_USE_CACHE')
        if _DUMBEN_CACHE_BASE is not None:
            DUMBEN_CACHE_BASE = Path(_DUMBEN_CACHE_BASE)
            DUMBEN_CACHE_BASE.mkdir(parents=True, exist_ok=True)

            fhash = hashlib.md5(
                # add code of sqlite_dumben just in case we change logic
                db.read_bytes() + Path(__file__).read_bytes()
            ).hexdigest()

            dumben_cache = DUMBEN_CACHE_BASE / fhash
            if dumben_cache.exists():
                # TODO log it?
                shutil.copy(dumben_cache, output)
                return

        # if we output as db, just operate on that target database directly
        shutil.copy(db, output)
        _dumben_db(output)

        if dumben_cache is not None:
            shutil.copy(output, dumben_cache)
        return

    # otherwise, need to create a temporary db to operate on -- and after that can dump it to sql
    # TODO need to be careful, if there are BLOBs in the database they may be dumped as empty strings
    with TemporaryDirectory() as td:
        tdir = Path(td)
        tdb = Path(tdir) / 'tmp.db'
        run(db=db, output=tdb, output_as_db=True)
        if output is not None:
            with output.open('w') as out:
                subprocess.run(_sqlite(tdb, '.dump'), check=True, stdout=out)
        else:
            subprocess.run(_sqlite(tdb, '.dump'), check=True, stdout=sys.stdout)


def test_dumben(tmp_path: Path) -> None:
    # TODO would be nice to implement integration style test here straight away
    sql = '''
CREATE TABLE departments
( department_id INTEGER PRIMARY KEY AUTOINCREMENT,
  department_name VARCHAR
);

CREATE TABLE employees
( employee_id INTEGER PRIMARY KEY AUTOINCREMENT,
  last_name VARCHAR NOT NULL,
  first_name VARCHAR,
  department_id INTEGER,
  CONSTRAINT fk_departments
    FOREIGN KEY (department_id)
    REFERENCES departments(department_id)
    ON DELETE CASCADE
);

INSERT INTO departments VALUES (30, 'HR');
INSERT INTO departments VALUES (999, 'Sales');

INSERT INTO employees VALUES (10000, 'Smith', 'John', 30);
INSERT INTO employees VALUES (10001, 'Anderson', 'Dave', 999);

CREATE VIEW whatevs AS
    SELECT * FROM employees;
'''

    db = tmp_path / 'tmp.db'
    subprocess.run(_sqlite(db), input=sql.encode('utf8'), check=True)

    ## precondition -- check that db has multiline CREATE statements
    dbd = check_output(_sqlite(db, '.dump')).decode('utf8').splitlines()
    assert 'CREATE TABLE employees' in dbd
    assert '  CONSTRAINT fk_departments' in dbd
    ##

    ## precondition -- check that with foreign key it will indeed impact other tables
    check_call(_sqlite(db, 'PRAGMA foreign_keys=on; DELETE FROM departments WHERE department_id = 30;'))
    ecnt = int(check_output(_sqlite(db, 'SELECT COUNT(*) FROM employees')).decode('utf8').strip())
    assert ecnt == 1, ecnt
    ##

    db.unlink()
    subprocess.run(_sqlite(db), input=sql.encode('utf8'), check=True)

    dumb_sql = tmp_path / 'dumb.sql'
    run(db=db, output=dumb_sql, output_as_db=False)
    dump = dumb_sql.read_text()
    dump_lines = dump.splitlines()

    crt = dump_lines[5]  # meh but easiest
    # make sure it puts the statement on single line
    assert re.fullmatch(r'CREATE TABLE `employees` \(`employee_id` INTEGER,.*`department_id` INTEGER.*\);', crt)
    # make sure it strips off constraints
    assert 'AUTOINCREMENT' not in crt, crt
    assert 'CONSTRAINT' not in crt, crt

    assert 'CREATE VIEW' not in dump

    dumb_db = tmp_path / 'dumb.db'
    run(db=db, output=dumb_db, output_as_db=True)
    check_call(_sqlite(dumb_db, 'PRAGMA foreign_keys=on; DELETE FROM departments WHERE department_id = 30;'))
    ecnt = int(check_output(_sqlite(dumb_db, 'SELECT COUNT(*) FROM employees')).decode('utf8').strip())
    assert ecnt == 2, ecnt


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


# some possible inspiration for testing
# - KoboReader-20211130.sqlite seems to have
#    CREATE TRIGGER kobo_plus_asset_cleanup
# - fb messenger android is a good db to test on... lots of weird shit, e.g. transactions
# - bumble android has search_message_removed trigger
# - whatsapp android has loads of weird shit
