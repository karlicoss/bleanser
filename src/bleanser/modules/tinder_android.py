#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # TODO likely will need to enable true
    MULTIWAY = False
    DELETE_DOMINATED = False

    def cleanup(self, c) -> None:
        t = Tool(c)
        t.drop('instagram_broken_links')

        # some odd id that increases with no impact for other data
        t.drop_cols(table='profile_media', cols=['client_sequential_id'])

        # TODO I guess generally safer to drop specific columns instead of whole tables
        t.drop_cols(table='match_seen_state', cols=['match_id', 'last_message_seen_id'])

        # TODO match->last_activity_date -- hmmm changing quite a bit? is it interesting? not sure
        #
        # last_acitivy_date table? only has activity for yourself? not sure...
        #
        # message->is_liked -- not sure if worth keeping... only for finding out the first change?
        #
        # match_read_receipt -- what is it??
        # match_id	last_seen_message_id	seen_timestamp
        # seems that last last_seen_message_id can be restored from messages table... but seen_timestamp is unique?


if __name__ == '__main__':
    Normaliser.main()
