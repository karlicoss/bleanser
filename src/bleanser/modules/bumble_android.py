import json

from bleanser.core.modules.json import delkeys
from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        # fmt: off
        message   = tables['message']
        conv_info = tables['conversation_info']

        assert 'id'                in message
        assert 'conversation_id'   in message
        assert 'payload'           in message
        assert 'created_timestamp' in message

        assert 'user_id'   in conv_info
        assert 'user_name' in conv_info
        # fmt: on

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop('search_fts_segments')
        t.drop('search_fts_segdir')
        t.drop('search_fts_docsize')
        t.drop('search_fts_content')
        t.drop('search_fts_stat')
        t.drop('message_read_info')

        t.drop_cols('conversation_info', cols=[
            'user_image_url',
            'photo_url',
            'last_seen_message_id',
            'covid_preferences',

            'chat_input_settings',

            'match_status',  # ?? either NULL or -1 or some weird hash thing??

            'sending_multimedia_enabled',
            'disabled_multimedia_explanation',
        ])  # fmt: skip
        # for extract: photo_id can be a bit volatile

        # mm, user photos are a bit annoying, urls are flaky
        def _cleanup_jsons(s):
            if s is None:
                return None
            j = json.loads(s)
            delkeys(
                j,
                keys=[
                    'url',  # for conversation_info.user_photos & message.payload
                    'expiration_timestamp',  # for message.payload
                ],
            )
            return json.dumps(j)

        c.create_function("CLEANUP_JSONS", 1, _cleanup_jsons)
        list(c.execute('UPDATE conversation_info SET user_photos = CLEANUP_JSONS(user_photos)'))
        list(c.execute('UPDATE message           SET payload     = CLEANUP_JSONS(payload)'))


if __name__ == '__main__':
    Normaliser.main()
