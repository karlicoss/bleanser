from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


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
        assert 'played_duration' in media
        assert 'last_played_time' in media

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        # often changing, no point keeping
        t.drop_cols(
            table='Feeds',
            cols=[
                'last_update',
                'last_update_failed',
                'image_url',  # volatile
                'minimal_duration_filter',
            ],
        )

        t.drop_cols(
            table='FeedMedia',
            cols=[
                'download_url',  # sometimes change, especially tracking links -- probs not worth keeping anyway
                'filesize',  # no idea why would it change, but it does sometimes
            ],
        )

        t.drop_cols(
            table='FeedItems',
            cols=[
                'title',  # useful feed, but volatile so best to ignore
                'content_encoded',  # no idea what is it but volatile
                'description',  # often changing, no need to keep
                'image_url',  # volatile
            ],
        )

        t.drop('Queue')


if __name__ == '__main__':
    Normaliser.main()
