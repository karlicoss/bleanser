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
        check_table('bookmarks')
        check_table('visits')
        # moz_annos -- apparently, downloads?

    def cleanup(self, c: Connection) -> None:
        tool = Tool(c)
        # triggers some unnecessary checks, like guid check in bookmarks
        tool.drop_index('bookmarks_guid_index')
        # tool.drop_view('combined')
        # tool.drop_view('combined_with_favicons')
        # tool.drop_view('bookmarks_with_annotations')
        # tool.drop_view('bookmarks_with_favicons')

        tool.drop_cols(
            table='history',
            cols=[
                # aggregates, changing all the time
                'visits',
                'visits_local',
                'visits_remote',
                ##

                # hmm, this seems to be last date.. actual dates are in 'visits'
                'date',
                'date_local',
                'date_remote',
                ##

                'title',  # ugh. changes dynamically

                'modified', # ???
            ]
        )
        tool.drop_cols(
            table='clients',
            cols=['last_modified'],
        )
        tool.drop_cols(
            table='bookmarks',
            # we don't care about these
            cols=[
                'position', 'localVersion', 'syncVersion',
                'modified', # also seems to depend on bookmark position

                'guid',  # sort of a hash and changes with position changes too?
            ],
        )

        # logs everything and changes all the time ... not really interesting
        tool.drop('remote_devices')

        tool.drop('thumbnails')

        # doesn't really have anything interesting? ...
        # just some image urls and maybe titles... likely no one cares about them
        tool.drop('page_metadata')

        # FIXME ????
        tool.drop('tabs')


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser)
