import json

from bleanser.core.modules.json import delkeys
from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


def _cleanup_json_dict(s: str | None, *, keys: tuple[str, ...]) -> str | None:
    if s is None or s == '':
        return s

    j = json.loads(s)
    assert isinstance(j, dict), j
    delkeys(j, keys=keys)
    return json.dumps(j, sort_keys=True, separators=(',', ':'))


def _cleanup_msg_local_info(s: str | None) -> str | None:
    return _cleanup_json_dict(
        s,
        keys=(
            'feed_video_last_query_time',  # Timestamp of the last cover-cache query.
            'feed_video_last_unavailable_pid',  # Identifier from the last failed cover lookup.
            'feed_video_cover_url',  # Expiring CDN cover URL.
            'feed_video_origin_cover_url',  # Expiring CDN original-cover URL.
            'key_content_avatar',  # Expiring profile-avatar URL.
            'key_local_ext_has_read',  # Local read flag duplicated by the conversation read index.
            'video_sticker_status',  # Cached video-sticker availability.
            'IS_FETCHED',  # Local fetch-completion flag.
            'content_understanding_tag',  # Derived content classification.
            'SUGGESTED_REPLIES',  # Generated reply suggestions.
            'SUG_REPLY_SHOW_TIME',  # Suggestion UI display timestamp.
            'is_inline_dismissed',  # Local suggestion-dismissal state.
            's:get_msg_log_id',  # Request diagnostic log identifier.
        ),
    )


def _cleanup_conversation_core_ext(s: str | None) -> str | None:
    return _cleanup_json_dict(s, keys=('s:sync_event_time', 's:from_mt_sync'))


def _cleanup_conversation_setting_ext(s: str | None) -> str | None:
    return _cleanup_json_dict(s, keys=('s:sync_setting',))


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True
    CUSTOM_TOKENIZERS = frozenset({'mmicu'})

    ALLOWED_BLOBS = frozenset({
        ('msg', 'content_pb'),
    })  # fmt: skip

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        messages = tables['msg']
        assert 'msg_uuid' in messages
        assert 'content' in messages

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)

        # Dumben removes the virtual FTS tables themselves before cleanup, leaving these backing tables.
        t.drop(
            'im_search_content_official',  # Canonical messages can rebuild this full-text search content.
            'im_search_index_official_docsize',  # FTS document sizes are derived search bookkeeping.
            'im_search_index_official_segdir',  # FTS segment directories are derived search bookkeeping.
            'im_search_index_official_segments',  # FTS segments are derived from canonical messages.
            'im_search_index_official_stat',  # FTS statistics are derived search bookkeeping.
        )
        t.drop(
            'im_search_index_new_content',  # Canonical messages can rebuild this full-text search content.
            'im_search_index_new_docsize',  # FTS document sizes are derived search bookkeeping.
            'im_search_index_new_segdir',  # FTS segment directories are derived search bookkeeping.
            'im_search_index_new_segments',  # FTS segments are derived from canonical messages.
            'im_search_index_new_stat',  # FTS statistics are derived search bookkeeping.
        )

        tables = t.get_tables()
        if 'pending_message_body' in tables and t.count('pending_message_body') == 0:
            t.drop('pending_message_body')  # An empty receive queue contains no pending message history.
        if 'retry_request' in tables and t.count('retry_request') == 0:
            t.drop('retry_request')  # An empty retry queue contains no pending request history.
        if 'status_message_result' in tables and t.count('status_message_result') == 0:
            t.drop('status_message_result')  # An empty diagnostic queue contains no processing history.

        t.drop_cols(
            'conversation_list',
            cols=[
                'last_msg_index',  # Materialized from canonical message indices.
                'updated_time',  # Materialized from the latest canonical message timestamp.
                'unread_count',  # Derived from the retained read index and canonical messages.
                'ticket',  # Ephemeral server access token.
                'local_info',  # Device-local ranking, read-time, and UI bookkeeping.
                'has_more',  # Server pagination state.
                'member_count',  # Materialized from the participant table.
                'last_msg_order_index',  # Materialized from canonical message ordering.
                'sort_order',  # Derived conversation ranking.
                'min_index',  # Server synchronization cursor.
                'min_index_v2',  # Server synchronization cursor.
                'max_index_v2',  # Server synchronization cursor.
                'badge_count',  # Derived unread badge counter.
                'read_badge_count',  # Derived unread badge counter.
                'last_msg_uuid',  # Materialized from the latest canonical message.
                'badge_version',  # Derived badge counter version.
                'fake_unread_count',  # Derived unread counter.
                'conv_rank_version',  # Derived ranking state version.
            ],
        )
        t.drop_cols('msg', cols=['read_status'])  # Duplicated by the retained conversation read index.
        t.drop_cols('conversation_core', cols=['info_version'])  # Server synchronization version.
        t.drop_cols('conversation_setting', cols=['info_version'])  # Server synchronization version.

        c.create_function('CLEANUP_MSG_LOCAL_INFO', 1, _cleanup_msg_local_info)
        c.create_function('CLEANUP_CONVERSATION_CORE_EXT', 1, _cleanup_conversation_core_ext)
        c.create_function('CLEANUP_CONVERSATION_SETTING_EXT', 1, _cleanup_conversation_setting_ext)
        c.execute('UPDATE msg SET local_info = CLEANUP_MSG_LOCAL_INFO(local_info)')
        c.execute('UPDATE conversation_core SET ext = CLEANUP_CONVERSATION_CORE_EXT(ext)')
        c.execute('UPDATE conversation_setting SET ext = CLEANUP_CONVERSATION_SETTING_EXT(ext)')
        c.execute("DELETE FROM conversation_kv WHERE key = 'reshow_as_inner_push_time'")  # Notification delivery state.


if __name__ == '__main__':
    Normaliser.main()
