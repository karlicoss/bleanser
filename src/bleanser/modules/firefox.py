from sqlite3 import Connection

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def is_old_firefox(self, c: Connection) -> bool:
        tool = Tool(c)
        tables = tool.get_tables()
        if 'bookmarks' in tables:
            return True
        if 'moz_bookmarks' in tables:
            return False
        raise RuntimeError(f"Unexpected schema {tables}")

    def check(self, c: Connection) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        # fmt: off
        if self.is_old_firefox(c):
            v = tables['visits']
            assert 'history_guid' in v, v
            assert 'date'         in v, v

            h = tables['history']
            assert 'url'  in h, h
            assert 'guid' in h, h
        else:
            b = tables['moz_bookmarks']
            assert 'dateAdded' in b, b
            assert 'guid'      in b, b

            h = tables['moz_historyvisits']
            assert 'place_id'   in h, h
            assert 'visit_date' in h, h

            p = tables['moz_places']
            assert 'url' in p, p
            assert 'id'  in p, p
        # fmt: on

    def cleanup(self, c: Connection) -> None:
        self.check(c)

        if self.is_old_firefox(c):
            self.cleanup_old(c)
            return

        # otherwise, assume new db format

        tool = Tool(c)
        [(visits_before,)] = c.execute('SELECT count(*) FROM moz_historyvisits')
        tool.drop_cols(
            table='moz_places',
            cols=[
                # aggregates, changing all the time
                'frecency',
                'recalc_frecency',
                'alt_frecency',
                'recalc_alt_frecency',
                'last_visit_date',
                'visit_count',
                # ugh... sometimes changes because of notifications, e.g. twitter/youtube?, or during page load
                'hidden',
                'typed',
                'title',
                'description',
                'preview_image_url',
                'foreign_count',  # just some internal refcount thing... https://bugzilla.mozilla.org/show_bug.cgi?id=1017502
                ## mobile only
                'visit_count_local',
                'last_visit_date_local',
                'last_visit_date_remote',
                'sync_status',
                'sync_change_counter',
                ##
                ## ? maybe mobile only
                'visit_count_remote',
                ##
            ],
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
        # tool.drop('moz_annos')  # not sure -- contains downloads data? might be volatile

        tool.drop_cols(
            'moz_inputhistory',
            cols=[
                'use_count',  #  eh, some floating point that changes all the time
            ],
        )

        tool.drop_cols(
            'moz_bookmarks_synced',
            cols=[
                'id',  # id always changes, and they have guid instead
                'serverModified',  # changes without any actual changes to bookmark?
            ],
        )

        ## fenix
        tool.drop_cols(
            'moz_bookmarks_synced_structure',
            cols=[
                # I think it's the position in bookmarks list, doesn't matter
                'position',
            ],
        )
        tool.drop('moz_places_metadata_search_queries')

        tool.drop_cols(
            'moz_places_metadata',
            cols=[
                ## volatile
                'updated_at',
                'total_view_time',
                'typing_time',
                'key_presses',
                'scrolling_time',
                'scrolling_distance',
                ##
            ],
        )
        ##

        # TODO do we still need it?
        # sanity check just in case... can remove after we get rid of triggers properly...
        [(visits_after,)] = c.execute('SELECT count(*) FROM moz_historyvisits')
        assert visits_before == visits_after, (visits_before, visits_after)

    def cleanup_old(self, c) -> None:
        tool = Tool(c)

        # TODO could be pretty useful + really marginal benefits form cleaning it up, like 5% of databases maybe
        # tool.drop('searchhistory')

        tool.drop('thumbnails')
        tool.drop('favicons')

        # doesn't really have anything interesting? ...
        # just some image urls and maybe titles... likely no one cares about them
        tool.drop('page_metadata')

        tool.drop_cols(
            'bookmarks',
            # we don't care about these
            cols=[
                'position',
                'localVersion',
                'syncVersion',
                'modified',  # also seems to depend on bookmark position
                'guid',  # sort of a hash and changes with position changes too?
            ],
        )
        tool.drop_cols(
            'clients',
            cols=['last_modified'],
        )
        tool.drop_cols(
            'history',
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
                'title',
                # ugh. changes dynamically. e.g. (1) on twitter/telegram notifications
                # could update in some elaborate manner. idk
                'modified',  # ? changes for no apparent reason, probs because of the corresponding aggregates
            ],
        )

        tool.drop_cols(
            'remote_devices',
            cols=[
                # probs only the presence of devices is interesting..
                # changing all the time for no reason
                '_id',
                'modified',
                'last_access_time',
                'created',  # yes, this also changed all the time
            ],
        )

        # FIXME hmm...
        # on the one hand, kind of interesting info..
        # on the other, they change A LOT, so we'll miss most of tab snapshots anyway...
        # also newer databases don't have tab information anyway.. so I guess for now best to clean them up..
        tool.drop('tabs')
        # tool.drop_cols(
        #     'tabs',
        #     cols=['_id', 'favicon', 'position',],
        # )


if __name__ == '__main__':
    Normaliser.main()


# TODO need to make sure we test 'rolling' visits
# these look like they are completely cumulative in terms of history
def test_fenix() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2

    res = actions2(path=TESTDATA / 'fenix', rglob='**/*.sqlite*', Normaliser=Normaliser)
    assert res.remaining == [
        # eh, too lazy to document the reason for keeping them...
        # many of them are just bookmark changes
        '20210327103953/places.sqlite',
        '20210408155753/places.sqlite',
        '20210419092604/places.sqlite',
        '20210514081246/places.sqlite',
        # '20210517094437/places.sqlite',  # move
        # '20210517175309/places.sqlite',  # move
        # '20210520132446/places.sqlite',  # move
        # '20210522092831/places.sqlite',  # move
        # '20210524152154/places.sqlite',  # move
        # '20210526075434/places.sqlite',  # move
        # '20210527062123/places.sqlite',  # move
        # '20210530172804/places.sqlite',  # move
        # '20210601165208/places.sqlite',  # move
        # '20210602192530/places.sqlite',  # move
        # '20210603032923/places.sqlite',  # move
        '20210603144405/places.sqlite',
        '20210623234309/places.sqlite',
        '20210717141629/places.sqlite',
    ]
