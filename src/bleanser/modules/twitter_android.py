"""
Normalises data for official twitter Android app
"""

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tool = Tool(c)
        tables = tool.get_tables()

        statuses = tables['statuses']
        assert 'status_id' in statuses
        assert 'content' in statuses

        total_statuses = tool.count('statuses')
        if total_statuses == 0:
            # sometimes the database is completely empty (possibly some app shenanigans)
            # seems easiest to just skip checks for such dbs
            total_count = sum(tool.count(t) for t in tables if t != 'android_metadata')
            assert total_count == 0, "database seems broken, but not completely empty"
        else:
            assert total_statuses > 10  # sanity check

        [(statuses_without_content,)] = c.execute('SELECT COUNT(*) FROM statuses WHERE content IS NULL')
        # another sanity check -- to make sure the content is actually stored in this column and not lost during migrations
        assert statuses_without_content == 0

        _timeline = tables['timeline']

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)

        # some sort of crappy analytics -- A LOT of it
        # I actually suspect it's the bulk of this database? removing it makes cleanup considerably faster
        t.drop('feedback_action')
        t.drop('timeline_feedback_actions')

        t.drop('promoted_retry')

        t.drop('card_state')  # only has a couple of rows which are always changing.. some policy crap

        t.drop('status_groups')  # doesn't looks like anything interesting, contains read state?

        # seems like it contains last retweet for each tweet or something.. doesn't actually have tweet data
        t.drop('retweets')

        t.drop('tokens')  # some internal thing

        t.drop_cols(
            'statuses',
            cols=[
                '_id',  # internal id
                ## volatile
                'favorite_count',
                'retweet_count',
                'view_count_info',
                'reply_count',
                'bookmark_count',
                'quote_count',
                'tweet_source',  # sometimes NULL at first?
                'flags',
                'self_thread_id',
                'edit_control',  # no idea what it is
                'unmention_info',  # no idea, some binary crap (not even text)
                'quick_promote_eligibility',
                'quoted_status_permalink',
                'conversation_control',
                ##
                #
                'r_ent_content',  # contains same data as 'content'
                #
                # cards contain some extra data embedded from the website (e.g. preview)
                # might be actually useful to extract data from it
                'card',
                'unified_card',
            ],
        )

        # NOTE: in principle tweet data is all in statues table
        # but we need timeline to reconstruct some feeds (e.g. users own tweets)
        t.drop_cols(
            'timeline',
            cols=[
                '_id',  # internal id
                ## volatile
                'is_read',
                'sort_index',
                'timeline_chunk_id',
                'updated_at',
                'scribe_content',  # some "for you" crap
                'created_at',  # internal created at, not tweet's
                'feedback_action_prompts',
                'social_context',
                'is_linger_impressed',
                'dismissed',
                ##
            ],
        )

        c.execute('''
        DELETE FROM timeline
        WHERE entity_group_id LIKE "%cursor%"
           OR entity_group_id LIKE "%who-to-follow%"
           OR entity_group_id LIKE "%trends%"
           OR entity_group_id LIKE "%semantic%"
           OR entity_group_id LIKE "%promoted%"
           OR entity_group_id LIKE "%home-conversation%"
           OR entity_group_id LIKE "%notification%"
           OR entity_id       LIKE "%trends%"
           OR entity_id       LIKE "%superhero%"
        ''')

        # after all decided to drop 'timeline' completely.. all actual data is in statuses table anyway
        # - the vast majority of volatile entrites in it are type == 17 (not sure what it is)
        # - it also contains non-user timelines (e.g. when you open someone's profile in twitter app)
        t.drop('timeline')

        t.drop('users')  # they change all the time and probs not worth keeping all changes

        ## they are empty most of the time? sometimes contains an odd item for some reason
        t.drop('user_groups')
        t.drop('user_metadata')
        ##

        def remove_volatile_content(s):
            if s is None:
                return None
            xxx = s.find(bytes.fromhex('00695858583869306938306938496a'))
            if xxx == -1:
                return s
            else:
                return s[:xxx]
                # if b'movie trailer' in s:
                print(s.hex(), type(s))
            return s

        # ugh... a few tweets sometimes have some binary changes??
        # also this doesn't seem to solve everything sadly.. so for now commenting
        # c.create_function('REMOVE_VOLATILE_CONTENT', 1, remove_volatile_content)
        # list(c.execute('UPDATE statuses SET content = REMOVE_VOLATILE_CONTENT(content)'))

        # so it's a bit shit, but content shouldn't really change, and seems too hard to filter out these changes in binary blobs here
        # except edited tweets? but I have a feeling editing is controlled by timeline.updated or something
        # either way it would be so rare it will likely be caught collaterally by other data changes
        c.execute("UPDATE statuses SET content = X'BABABA'")


if __name__ == '__main__':
    Normaliser.main()
