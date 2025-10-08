from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = frozenset({
        ('fts_virtual_episode_segments', 'block'),
        ('fts_virtual_episode_segdir', 'root'),
        ('fts_virtual_episode_docsize', 'size'),
        ('fts_virtual_episode_stat', 'value'),
    })  # fmt: skip

    # TODO this would be useful as a base class method
    # could be called before cleanup/extract etc
    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        assert 'podcasts' in tables, tables
        eps = tables['episodes']
        # to make sure it's safe to use multiway/prune dominated:
        assert 'playbackDate' in eps
        assert 'position_to_resume' in eps

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        ## often changing, no point keeping
        t.drop_cols(
            table='episodes',
            cols=[
                'thumbnail_id',
                'new_status',
                'downloaded_status_int',
                'thumbsRating',
            ],
        )

        # no point tracking podcasts we're not following
        c.execute('DELETE FROM podcasts WHERE subscribed_status = 0')

        t.drop_cols(
            table='podcasts',
            cols=[
                ## volatile at times, a bit annoying
                'author',
                'description',
                ##
                'last_modified',
                'etag',  # ?? sometimes contains quoted last_modified or something..
                'rating',
                'reviews',
                'iTunesID',
                'latest_publication_date',
                'averageDuration',
                'frequency',
                'episodesNb',
                'subscribers',
                'thumbnail_id',
                'update_date',
                'update_status',
                'filter_chapter_excluded_keywords',
                'category',
                'explicit',
                'server_id',
            ],
        )
        ##

        ## changing often and likely not interesting
        t.drop('ad_campaign')
        t.drop('bitmaps')
        t.drop('blocking_services')
        t.drop('content_policy_violation')
        t.drop('fts_virtual_episode_stat')
        t.drop('fts_virtual_episode_docsize')
        t.drop('fts_virtual_episode_segments')
        t.drop('fts_virtual_episode_segdir')
        t.drop('ordered_list')  # just some random numbers, always changing
        t.drop('statistics')  # just some random numbers, mostly empty
        t.drop('radio_search_results')
        t.drop('topics')  # some random topic names.. at some point just disappeared
        t.drop('iha')  # no idea what is it, contains one entry sometimes; volatile

        ## probably unnecessary?
        # tool.drop('chapters')
        # tool.drop('teams')
        # tool.drop('topics')
        # tool.drop('relatedPodcasts')
        # tool.drop('content_policy_violation')  # lol
        ##


if __name__ == '__main__':
    Normaliser.main()


def test_podcastaddict() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2

    res = actions2(path=TESTDATA / 'podcastaddict_android', rglob='**/*.db*', Normaliser=Normaliser)
    assert res.remaining == [
        '20180106220736/podcastAddict.db',
        '20190227212300/podcastAddict.db',
        '20200217195816/podcastAddict.db',

        '20200406041500/podcastAddict.db',
        # '20210306070017/podcastAddict.db',
        # '20210306070020/podcastAddict.db',
        '20210306140046/podcastAddict.db',

        # keep: episode position changed
        '20210306165958/podcastAddict.db',

        # '20210509141916/podcastAddict.db',
        # '20210510070001/podcastAddict.db',
        # '20210511185801/podcastAddict.db',
        '20210513164819/podcastAddict.db',
        # some podcast lengths changed... might be useful
        '20210517000609/podcastAddict.db',
        # '20211226145720/podcastAddict.db',
        # '20211226172310/podcastAddict.db',
        # some podcast authors changed... dunno if useful but whatever
        '20211228010151/podcastAddict.db',
    ]  # fmt: skip
