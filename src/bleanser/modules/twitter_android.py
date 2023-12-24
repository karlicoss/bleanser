"""
Normalises data for official twitter Android app
"""
from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        statuses = tables['statuses']
        assert 'status_id' in statuses
        assert 'content' in statuses

        timeline = tables['timeline']

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)

        # some sort of crappy analytics -- A LOT of it
        # I actually suspect it's the bulk of this database? removing it makes cleanup considerably faster
        t.drop('feedback_action')
        t.drop('timeline_feedback_actions')

        t.drop('card_state')  # only has a couple of rows which are always changing.. some policy crap

        t.drop('status_groups')  # doesn't looks like anything interesting, contains read state?

        t.drop('retweets')  # seems like it contains last retweet for each tweet or something.. doesn't actually have tweet data

        t.drop_cols('statuses', cols=[
            '_id',  # internal id

            ## volatile
            'favorite_count',
            'retweet_count',
            'view_count_info',
            'reply_count',
            'bookmark_count',
            'quote_count',

            'edit_control',  # no idea what it is
            ##

            'r_ent_content',  # contains same data as 'content'
        ])

        t.drop_cols('users', cols=[
            '_id',  # internal id

            ## volatile
            'followers',
            'friends',
            'statuses',
            'favorites',
            'media_count',
            'updated',
            'hash',
            'user_flags',
            'advertiser_type',
            'url_entities',
            'pinned_tweet_id',
            'description',
            'image_url',
            'header_url',
            'profile_image_shape',
            'professional',
            'user_label_data',
            'verified_type',
            ##
        ])

        t.drop_cols('timeline', cols=[
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
        ])

        c.execute('''
        DELETE FROM timeline
        WHERE entity_group_id LIKE "%cursor%"
           OR entity_group_id LIKE "%who-to-follow%"
           OR entity_group_id LIKE "%trends%"
           OR entity_group_id LIKE "%semantic%"
           OR entity_group_id LIKE "%promoted%"
           OR entity_group_id LIKE "%home-conversation%"
           OR entity_id       LIKE "%trends%"
        ''')


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


if __name__ == '__main__':
    Normaliser.main()
