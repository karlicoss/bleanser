#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection
from typing import List


from bleanser.core import logger
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import SqliteNormaliser


class Normaliser(SqliteNormaliser):
    DELETE_DOMINATED = True
    MULTIWAY = True

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
        # FIXME quoting
        def drop(table: str) -> None:
            c.execute(f'DROP TABLE IF EXISTS {table}')

        def update(table: str, **kwargs) -> None:
            kws = ', '.join(f'{k}=?' for k, v in kwargs.items())
            c.execute(f'UPDATE {table} set {kws}', list(kwargs.values()))

        def drop_cols(*, table: str, cols: List[str]) -> None:
            update(table, **{col: '' for col in cols})
            # TODO crap. https://stackoverflow.com/a/66399224/706389
            # alter table is since march 2021... so won't be in sqlite for a while
            # for col in cols:
            #     c.execute(f'ALTER TABLE {table} DROP COLUMN {col}')

        drop_cols(
            table='moz_places',
            cols=[
                # aggregates, changing all the time
                'frecency',
                'last_visit_date',
                'visit_count',
                # ugh... sometimes changes because of notifications, e.g. twitter/youtube?, or during page load
                'title',
                'description',
                'preview_image_url',
            ]
        )
        drop_cols(
            table='moz_bookmarks',
            cols=['lastModified'],  # changing all the time for no reason?
        )
        drop('moz_meta')
        drop('moz_origins')  # prefix/host/frequency -- not interesting
        # todo not sure...
        drop('moz_inputhistory')


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser, glob='*.sqlite')
