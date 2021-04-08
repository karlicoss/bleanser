#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection


from bleanser.core import logger
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    DELETE_DOMINATED = True
    # TODO hmm.. unclear why is multiway quite a bit beter for it?
    # cleaned 95/137 files vs 40/137
    # MULTIWAY = True

    def __init__(self, db: Path) -> None:
        # todo not sure about this?.. also makes sense to run checked for cleanup/extract?
        with self.checked(db) as conn:
            self.tables = get_tables(conn)
        assert 'podcasts' in self.tables, self.tables
        assert 'episodes' in self.tables, self.tables

    def cleanup(self, c: Connection) -> None:
        tool = Tool(c)
        ## often changing
        tool.drop_cols(table='episodes', cols=['thumbnail_id'])
        tool.drop_cols(table='podcasts', cols=['update_date', 'episodesNb', 'thumbnail_id', 'subscribers'])
        ##

        tool.drop('ordered_list')  # just some random numbers, always changing
        tool.drop('sqlite_stat1')  # ???
        ## changing often an likely not interesting
        tool.drop('blocking_services')
        tool.drop('ad_campaign')
        tool.drop('bitmaps')
        tool.drop('fts_virtual_episode_docsize')
        tool.drop('fts_virtual_episode_segments')
        tool.drop('fts_virtual_episode_segdir')
        ## probably unnecessary?
        tool.drop('chapters')
        tool.drop('teams')
        tool.drop('topics')
        tool.drop('radio_search_results')
        tool.drop('relatedPodcasts')
        tool.drop('content_policy_violation')  # lol
        ##


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser)
