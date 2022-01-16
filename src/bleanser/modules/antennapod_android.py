#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True


    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        assert 'Feeds' in tables, tables
        eps = tables['FeedItems']
        assert 'link' in eps
        assert 'read' in eps

        # should be safe to use multiway because of these vvv
        media = tables['FeedMedia']
        assert 'played_duration'  in media
        assert 'last_played_time' in media


    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        # often changing, no point keeping
        t.drop_cols(table='Feeds', cols=[
            'last_update',
        ])


if __name__ == '__main__':
    Normaliser.main()

