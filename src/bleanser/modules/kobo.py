#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection

from bleanser.core import logger
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    DELETE_DOMINATED = True

    def __init__(self, db: Path) -> None:
        # todo not sure about this?.. also makes sense to run checked for cleanup/extract?
        with self.checked(db) as conn:
            self.tables = get_tables(conn)
        def check_table(name: str) -> None:
            assert name in self.tables, self.tables
        check_table('content')
        check_table('Bookmark')
        check_table('BookAuthors')

    def cleanup(self, c: Connection) -> None:
        tool = Tool(c)
        tool.drop('content') # some cached book data? so not very interesting when it changes..
        tool.drop('content_keys')  # just some image meta
        tool.drop('volume_shortcovers')  # just some hashes
        tool.drop('volume_tabs')  # some hashes
        # ## often changing
        # c.execute('UPDATE episodes SET thumbnail_id=-1')
        # c.execute('UPDATE podcasts SET update_date=-1,episodesNb=-1,thumbnail_id=-1,subscribers=-1')
        # ##


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser)
