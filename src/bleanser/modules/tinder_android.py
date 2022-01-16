#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = {
        ('contextual_match', 'by_opener'),
        ('contextual_match', 'by_closer'),
        *(('match_person', x) for x in ['gender', 'photos', 'badges', 'jobs', 'schools', 'city']),
        ('pending_media', 'media_template'),
        ('profile_descriptor', 'descriptor'),
        ('profile_media', 'media_template'),
        ('top_pick_teaser', 'tags'),
        ('sponsored_match_creative_values', 'photos'),
        ('sponsored_match_creative_values', 'match_screen_image'),
        *(('profile_user', x) for x in ['gender', 'photos', 'badges', 'jobs', 'schools', 'city', 'sexual_orientations']),
        ('profile_add_loop', 'loops'),
        ('instagram_new_media', 'media'),
        ('profile_change_school', 'schools'),
        ('activity_feed_artist', 'images'),
        ('activity_feed_album', 'images'),
        ('profile_change_work', 'works'),
        ('profile_add_photo', 'photos'),
        ('instagram_connect', 'photos'),
    }


    def cleanup(self, c) -> None:
        t = Tool(c)
        t.drop('instagram_broken_links')

        # some odd id that increases with no impact for other data
        t.drop_cols(table='profile_media', cols=['client_sequential_id'])

        # TODO I guess generally safer to drop specific columns instead of whole tables
        t.drop_cols(table='match_seen_state', cols=['match_id', 'last_message_seen_id'])

        # TODO last_acitivy_date table? only has activity for yourself? not sure...
        # t.drop('last_activity_date')
        # this one contributes to _a lot_ of changes, dunno
        #
        # match->last_activity_date -- hmmm changing quite a bit? is it interesting? not sure
        #
        # message->is_liked -- not sure if worth keeping... only for finding out the first change?
        #
        # match_read_receipt -- what is it??
        # match_id	last_seen_message_id	seen_timestamp
        # seems that last last_seen_message_id can be restored from messages table... but seen_timestamp is unique?


if __name__ == '__main__':
    Normaliser.main()


def test_tinder() -> None:
    from bleanser.tests.common import skip_if_no_data; skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2
    res = actions2(path=TESTDATA / 'tinder_android', rglob='*.db*', Normaliser=Normaliser)

    assert res.remaining == [
        '20210523193545/tinder-3.db',  # keep, first in group
        # '20210916214349/tinder-3.db',  # MOVE
        # '20210916223254/tinder-3.db',  # MOVE
        '20210916232749/tinder-3.db',  # keep, some likes changes etc
        '20210917004827/tinder-3.db',
        '20210917014719/tinder-3.db',
        '20210917015444/tinder-3.db',
        # '20210917031235/tinder-3.db',  # MOVE
        '20210917060029/tinder-3.db',


        '20211007060802/tinder-3.db',  # keep, first in group
        # '20211007090109/tinder-3.db',
        # '20211007094056/tinder-3.db',
        '20211007115318/tinder-3.db',  # keep, last_activity
        # '20211007133114/tinder-3.db',
        # '20211007143940/tinder-3.db',
        # '20211007155908/tinder-3.db',
        # '20211007165243/tinder-3.db',
        '20211007180708/tinder-3.db',  # keep, bio changed

        '20211225050314/tinder-3.db',  # keep: first in group
        # '20211225193930/tinder-3.db',
        '20211226052237/tinder-3.db',  # keep: last_activity_date changed
        # '20211226091116/tinder-3.db',
        # '20211226135158/tinder-3.db',
        # '20211227002918/tinder-3.db',
        '20211227044403/tinder-3.db',  # keep: last_actvivity_date changed
        '20211227145813/tinder-3.db',  # keep: last in group
    ]
