from sqlite3 import Connection

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c: Connection) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        matches = tables['match']
        assert 'person_id' in matches, matches

        messages = tables['message']
        assert 'text' in messages, messages
        assert 'match_id' in messages, messages

    def cleanup(self, c: Connection) -> None:
        self.check(c)

        t = Tool(c)

        t.drop(
            'instagram_broken',
            'explore_attribution',
            #
            ## messages from Tinder itself
            'inbox_message',
            'inbox_message_images',
            'inbox_message_text_formatting',
            ##
        )

        # eh, don't think it impacts anyway
        # t.drop('contextual_match')
        # it contains some photos? dunno

        # some odd id that increases with no impact for other data
        t.drop_cols(table='profile_media', cols=['client_sequential_id'])

        t.drop_cols(table='match_seen_state', cols=['match_id', 'last_message_seen_id'])

        t.drop('match_your_turn_state')

        # TODO profile_descriptor?? blob containing presumably profile info, and sometimes jumps quite a bit

        # this one contributes to _a lot_ of changes, like 40%
        # and I guess if we properly wanted to track when app was activated, we'd need a different mechanism anyway
        t.drop('last_activity_date')

        # hmm what is match_harassing_message??

        # TODO not sure about this?
        # t.drop_cols('match', cols=[
        #     'last_activity_date',
        # ])

        # TODO profile_descriptor changes quite a lot? not sure

        # match->last_activity_date -- hmmm changing quite a bit? is it interesting? not sure
        #
        # message->is_liked -- not sure if worth keeping... only for finding out the first change?
        #
        # match_read_receipt -- what is it??
        # match_id	last_seen_message_id	seen_timestamp
        # seems that last last_seen_message_id can be restored from messages table... but seen_timestamp is unique?

        # NOTE: for 'extract' mode
        # match->is_blocked


if __name__ == '__main__':
    Normaliser.main()


def test_tinder() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2

    res = actions2(path=TESTDATA / 'tinder_android', rglob='**/*.db*', Normaliser=Normaliser)

    assert res.remaining == [
        '20210523193545/tinder-3.db',  # keep, first in group
        # '20210916214349/tinder-3.db',  # MOVE
        # '20210916223254/tinder-3.db',  # MOVE
        '20210916232749/tinder-3.db',  # keep, some likes changes etc
        '20210917004827/tinder-3.db',
        '20210917014719/tinder-3.db',
        # '20210917015444/tinder-3.db',
        # '20210917031235/tinder-3.db',  # MOVE
        '20210917060029/tinder-3.db',


        '20211007060802/tinder-3.db',  # keep, first in group
        # '20211007090109/tinder-3.db',
        # '20211007094056/tinder-3.db',
        # '20211007115318/tinder-3.db',
        # '20211007133114/tinder-3.db',
        # '20211007143940/tinder-3.db',
        # '20211007155908/tinder-3.db',
        '20211007165243/tinder-3.db',
        '20211007180708/tinder-3.db',  # keep, bio changed

        '20211225050314/tinder-3.db',  # keep: first in group
        # '20211225193930/tinder-3.db',
        # '20211226052237/tinder-3.db',
        # '20211226091116/tinder-3.db',
        # '20211226135158/tinder-3.db',
        # '20211227002918/tinder-3.db',
        # '20211227044403/tinder-3.db',
        '20211227145813/tinder-3.db',  # keep: last in group
    ]  # fmt: skip
