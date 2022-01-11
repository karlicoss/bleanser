#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # TODO do we really need multiway now?
    MULTIWAY = True
    DELETE_DOMINATED = True

    def check(self, c) -> None:
        tool = Tool(c)
        tables = tool.get_schemas()
        b = tables['moz_bookmarks']
        assert 'dateAdded' in b, b
        assert 'guid'      in b, b
        h = tables['moz_historyvisits']
        assert 'place_id'   in h, h
        assert 'visit_date' in h, h
        p = tables['moz_places']
        assert 'url' in p, p
        assert 'id'  in p, p
        # moz_annos -- apparently, downloads?

    def cleanup(self, c) -> None:
        self.check(c)

        tool = Tool(c)
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

                ## ? maybe mobile only
                'visit_count_remote',
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
        # tool.drop('moz_inputhistory')

        tool.drop_cols('moz_bookmarks_synced', cols=[
            'id',  # id always changes, and they have guid instead
            'serverModified',  # changes without any actual chagnes to bookmark?
        ])

        # TODO do we still need it?
        # sanity check just in case... can remove after we get rid of triggers properly...
        [(visits_after,)] = c.execute('SELECT count(*) FROM moz_historyvisits')
        assert visits_before == visits_after, (visits_before, visits_after)


if __name__ == '__main__':
    Normaliser.main()


# TODO need to make sure we test 'rolling' visits
# these look like they are completely cumulative in terms of history
def test_fenix() -> None:
    from bleanser.tests.common import TESTDATA, actions2
    res = actions2(path=TESTDATA / 'fenix', rglob='*.sqlite*', Normaliser=Normaliser)
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
