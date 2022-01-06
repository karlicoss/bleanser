#!/usr/bin/env python3
import sys
import re
db = sys.argv[1]

import sqlite3
from subprocess import check_output

# todo not sure if should mess with utf8?


def process():
    
    schemas = {}
    with sqlite3.connect(f'file:{db}?immutable=1', uri=True) as conn:
        tables = []
        for row in conn.execute('SELECT name FROM sqlite_master WHERE type="table"'):
            [table] = row
            tables.append(table)

        for table in tables:
            schema = {}
            for row in conn.execute(f'PRAGMA table_info({table})'):
                col = row[1]
                type_ = row[2]
                schema[col] = type_
            schemas[table] = schema

    lines = check_output(['sqlite3', db, '.dump']).splitlines(keepends=True)
    g = []

    def dump():
        nonlocal g
        res =  b''.join(g)
        g = []

        allowed = [
            b'INSERT INTO ',
            b'DELETE FROM ',
            b'PRAGMA',
            b'BEGIN TRANSACTION',
            b'COMMIT',
        ]
        if any(res.startswith(s) for s in allowed):
            # meh
            if res.startswith(b'DELETE FROM sqlite_sequence') or res.startswith(b'INSERT INTO sqlite_sequence'):
               # smth to do with autoincrement
               return b''
            return res


        if res.startswith(b'CREATE TABLE '):
            si = res.find(b'(')
            assert si != -1, res
            # backticks are quotes (e.g. if table name is a special keyword...)
            table_name = res[len(b'CREATE TABLE '): si].strip().strip(b'`')
            schema = schemas[table_name.decode('utf8')]
            res2 = res[:si] + b' (' + b', '.join(f'{k} {v}'.encode('utf8') for k, v in schema.items()) + b');\n'
            return res2
            # jesus..
            # just drop it?


            # FIXME primary key might actually mention something...
            # res = res.replace(b'PRIMARY KEY', b'')
            # TODO ugh. there might be a bunch of other constraq
            res = res.replace(b'AUTOINCREMENT', b'')
            res = res.replace(b'NOT NULL', b'')
            # res = res.replace(b'FOREIGN KEY (message_id) REFERENCES message(id) ON DELETE CASCADE ON UPDATE CASCADE', b'')

            type_name = rb'(TEXT|NUMERIC|INTEGER|REAL|BLOB)'
            column_def = rb'\w+\s+' + type_name
            print(res)
            # FIXME fullmatch
            rgx = rb'CREATE TABLE \w+\s*\(\s*(\w+\s+' + type_name + rb'\s*,?\s*' + rb')+' + rb'\);'
            m = re.fullmatch(rgx, res[:-1])# ' \((\w+ ' + type_name + b', )+;', res[:-1])
            print(m)
            assert m is not None
            # see https://www.sqlite.org/lang_createtable.html
            # first, greedily match column defs
            # then ditch the rest -- it will only be constraints etc


            #m = re.fullmatch(rgx, res[:-1])
            #assert m is not None
            return res

        dropped = [
            b'CREATE INDEX ',
            b'CREATE VIEW ',
        ]
        if any(res.startswith(s) for s in dropped):
            return b''

        raise RuntimeError(res)

    for line in lines:
        if line.endswith(b';\n'):
            g.append(line)
            yield dump()
        else:
            g.append(line[:-1] + b' ')
    if len(g) > 0:
        yield dump()

import sys

for line in process():
    sys.stdout.buffer.write(line)
