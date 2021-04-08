#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection


from bleanser.core import logger
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import SqliteNormaliser, Tool


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
        tool = Tool(c)
        tool.drop_cols(
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
        tool.drop_cols(
            table='moz_bookmarks',
            cols=['lastModified'],  # changing all the time for no reason?
        )
        tool.drop('moz_meta')
        tool.drop('moz_origins')  # prefix/host/frequency -- not interesting
        # todo not sure...
        tool.drop('moz_inputhistory')


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser)
