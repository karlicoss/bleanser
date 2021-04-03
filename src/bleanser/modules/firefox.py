#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection
from typing import Optional


from bleanser.core.common import Relation, logger, relations_to_instructions
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import relations, SqliteNormaliser, sqlite_process


class Normaliser(SqliteNormaliser):
    DELETE_DOMINATED = True

    def __init__(self, db: Path) -> None:
        # todo not sure about this?.. also makes sense to run checked for cleanup/extract?
        with self.checked(db) as conn:
            self.tables = get_tables(conn)
        def check_table(name: str) -> None:
            assert name in self.tables, (name, self.tables)
        check_table('moz_bookmarks')
        check_table('moz_historyvisits')
        # moz_annos -- apparently, downloads?

    def cleanup(self, c: Connection) -> None:
        def drop(table: str) -> None:
            c.execute(f'DROP TABLE IF EXISTS {table}')

        def update(table: str, **kwargs) -> None:
            kws = ', '.join(f'{k}=?' for k, v in kwargs.items())
            c.execute(f'UPDATE {table} set {kws}', list(kwargs.values()))

        update(
            'moz_places',
            # aggregates, changing all the time
            frecency=-1,
            last_visit_date=-1,
            visit_count=-1,
            # ugh... sometimes changes because of notifications, e.g. twitter/youtube?, or during page load
            title='',
            description='',
            preview_image_url='',
        )
        update(
            'moz_bookmarks',
            lastModified=-1,  # changing all the time for no reason?
        )
        drop('moz_meta')
        drop('moz_origins')  # prefix/host/frequency -- not interesting
        # todo not sure...
        drop('moz_inputhistory')


import click
@click.command()
@click.argument('path', type=Path)
@click.option('--max-workers', required=False, type=int, help='Passed down to ThreadPoolExecutore. Use 0 for serial execution')
def main(path: Path, max_workers: Optional[int]) -> None:
    # TODO collect all sqlite mimes?
    paths = list(sorted(path.rglob('*.sqlite')))
    sqlite_process(paths, Normaliser=Normaliser, max_workers=max_workers)


if __name__ == '__main__':
    main()
