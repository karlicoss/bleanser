from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = frozenset({
        # hopefully should be fine, all the metadata seems to be present in the table
        ('chat_messages', 'serialized'),
        ('channels', 'serialized'),
    })  # fmt: skip

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        msgs = tables['chat_messages']
        # TODO hmm, maybe 'created' just means created in the db?
        assert 'sent' in msgs, msgs
        assert 'body' in msgs, msgs
        assert 'messageId' in msgs, msgs
        profiles = tables['profiles']
        assert 'userId' in profiles, profiles
        # not sure if really useful at all but whatever
        channels = tables['channels']
        assert 'subjectId' in channels, channels

    def cleanup(self, c) -> None:
        self.check(c)  # todo could also call 'check' after just in case
        t = Tool(c)
        # seems that e.g. liked_content has some retention, so will need multiway

        # TODO not sure if it can be useful at all?? it contains something like 'Today' etc...
        # it generates tons of changes.. so I'd rather drop it I guess
        t.drop_cols(table='profiles', cols=['lastActiveStatus', 'lastActiveStatusId'])

        # not sure what's the point of updated col here, it just changes for all entries at the same time
        t.drop_cols(table='channels', cols=['updated', 'serialized'])

        # eh, not sure, they appear to be modified without actual changes to other cols?
        # fmt: off
        t.drop_cols(table='profiles'     , cols=['created', 'updated', 'hidden'])
        t.drop_cols(table='answers'      , cols=['created', 'modified'])
        t.drop_cols(table='player_media' , cols=['created'])
        t.drop_cols(table='subject_media', cols=['created'])
        # fmt: on

        # instagram urls change all the time (they contain some sort of token)
        # and expire quickly anyway.. so just easier to cleanup
        c.execute('UPDATE subject_media SET photoUrl="", thumbnailUrl="", videoUrl="" WHERE source = "instagram"')
        # todo width,height are changing all the time for some reason for subject_media

        # TODO pending_ratings??

        ##
        t.drop(table='metrics')
        # TODO WTF?? they are collecting some network stats and putting in the db? e.g. metered/vpn/etc
        t.drop(table='networks')

        t.drop(table='preference_choices')  # search prefrences -- change all the time and not interesting
        t.drop(table='pending_ratings')  # flaky, seems like contains intermediate state

        ## clean up unnecessary profile/media data
        # seems 3 - seems like if there is a conversation with user, so worth keeping
        # state 1 - seems like 'liked', probs not worth tracking
        # state 11 is possibly 'seen', so not super interesting
        delete_profiles = 'FROM profiles WHERE state in (1, 11)'
        for tbl in ['subject_media', 'answers']:
            c.execute(f'DELETE FROM {tbl} WHERE userId IN (SELECT userId {delete_profiles})')
            # delete orphans too
            c.execute(f'DELETE FROM {tbl} WHERE userId NOT IN (SELECT userId FROM profiles)')
        c.execute(f'DELETE {delete_profiles}')
        ##

        ## id seems to be very unstable, as if they are resequenced all the time...
        remove_ids = [
            'answers',
            'player_media',
            'basic_choices',
            'branding',
            'channels',
            'surveys',
            'subject_media',
            'liked_content',
        ]
        for table in remove_ids:
            t.drop_cols(table=table, cols=['id'])

        # things are flaky here, even urls are changing between databases -- likely they are expiring
        t.drop(table='standouts_content')

        t.drop_cols(table='surveys', cols=['receivedByHinge'])
        t.drop_cols(table='call_prompt_packs', cols=['position'])
        # player_media are user pics? might be useful..
        t.drop_cols(table='player_media', cols=['position'])
        t.drop_cols(table='subject_media', cols=['position'])
        t.drop_cols(table='products', cols=['lastApiUpdate', 'lastStoreUpdate'])
        ##


if __name__ == '__main__':
    Normaliser.main()
