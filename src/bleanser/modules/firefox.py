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
        tool.drop_index('moz_places_guid_uniqueindex')
        tool.drop_index('guid_uniqueindex') # on mobile only
        [(visits_before,)] = c.execute('SELECT count(*) FROM moz_historyvisits')
        tool.drop_cols(
            table='moz_places',
            cols=[
                # aggregates, changing all the time
                'frecency',
                'last_visit_date',
                'visit_count',
                # ugh... sometimes changes because of notifications, e.g. twitter/youtube?, or during page load
                'hidden',
                'typed',
                'title',
                'description',
                'preview_image_url',

                'foreign_count', # jus some internal refcount thing... https://bugzilla.mozilla.org/show_bug.cgi?id=1017502

                ## mobile only
                'visit_count_local',
                'last_visit_date_local',
                'last_visit_date_remote',
                'sync_status',
                'sync_change_counter',
                ##
            ]
        )
        # ugh. sometimes changes for no reason...
        # and anyway, for history the historyvisits table refers place_id (this table's actual id)
        # also use update instead delete because phone db used to have UNIQUE constraint...
        c.execute('UPDATE moz_places SET guid=id')
        tool.drop_cols(
            table='moz_bookmarks',
            cols=['lastModified'],  # changing all the time for no reason?
            # todo hmm dateAdded might change when e.g. firefox reinstalls and it adds default bookmarks
            # probably not worth the trouble
        )
        tool.drop('moz_meta')
        tool.drop('moz_origins')  # prefix/host/frequency -- not interesting
        # todo not sure...
        tool.drop('moz_inputhistory')

        # sanity check just in case... can remove after we get rid of triggers properly...
        [(visits_after,)] = c.execute('SELECT count(*) FROM moz_historyvisits')
        assert visits_before == visits_after, (visits_before, visits_after)


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser)
