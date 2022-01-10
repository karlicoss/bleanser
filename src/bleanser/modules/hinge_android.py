#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    DELETE_DOMINATED = True


    def check(self, c) -> None:
        tables = Tool(c).get_schemas()
        msgs = tables['chat_messages']
        # TODO hmm, maybe 'created' just means created in the db?
        assert 'sent' in msgs, msgs
        assert 'body' in msgs, msgs
        assert 'messageId' in msgs, msgs
        profiles = tables['profiles']
        assert 'userId' in profiles, profiles


    def cleanup(self, c) -> None:
        self.check(c) # todo could also call 'check' after just in case
        t = Tool(c)
        # seems that e.g. liked_content has some retention, so will need multiway

        # TODO not sure if it can be useful at all?? it contains something like 'Today' etc...
        # it generates tons of changes.. so I'd rather drop it I guess
        t.drop_cols(table='profiles', cols=['lastActiveStatus'])

        # not sure what's the point of updated col here, it just changes for all entries at the same time
        t.drop_cols(table='channels', cols=['updated'])

        # eh, not sure, they appear to be modified without actual changes to other cols?
        t.drop_cols(table='profiles'     , cols=['created', 'updated'])
        t.drop_cols(table='answers'      , cols=['created', 'modified'])
        t.drop_cols(table='player_media' , cols=['created'])
        t.drop_cols(table='subject_media', cols=['created'])


        # instagram urls change all the time (they contain some sort of token)
        # and expire quickly anyway.. so just easier to cleanup
        c.execute('UPDATE subject_media SET photoUrl="" WHERE source = "instagram"')
        # todo width,height are changing all the time for some reason for subject_media

        # TODO pending_ratings??

        ##
        t.drop(table='metrics')
        # TODO WTF?? they are collecting some network stats and putting in the db? e.g. metered/vpn/etc
        t.drop(table='networks')

        ## id seems to be very unstable, as if they are resequenced all the time...
        remove_ids = [
            'pending_ratings',
            'answers',
            'player_media',
            'preference_choices',
            'basic_choices',
            'branding',
            'channels',
            'surveys',
            'subject_media',

            'liked_content',
        ]
        for table in remove_ids:
            t.drop_cols(table=table, cols=['id'])

        t.drop_cols(table='surveys', cols=['receivedByHinge'])
        t.drop_cols(table='standouts_content', cols=['position'])
        t.drop_cols(table='call_prompt_packs', cols=['position'])
        # player_media are user pics? might be useful..
        t.drop_cols(table='player_media', cols=['position'])
        t.drop_cols(table='subject_media', cols=['position'])
        t.drop_cols(table='products', cols=['lastApiUpdate', 'lastStoreUpdate'])
        ##



if __name__ == '__main__':
    Normaliser.main()
