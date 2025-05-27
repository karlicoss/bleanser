import json

from bleanser.core.modules.json import delkeys, patch_atoms
from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


def _patch_volatile_urls(x):
    # these contain some sort of hashes and change all the time
    if not isinstance(x, str):
        return x
    if 'fbcdn.net' in x:
        return ""
    if 'cdninstagram' in x:
        return ""
    return x


def _cleanup_jsons(s):
    if s is None:
        return None

    if isinstance(s, bytes):
        j = json.loads(s.decode('utf8'))
    else:
        # hmm normally it's bytes, but on odd occasions (old databases??) was str? odd
        j = json.loads(s)

    # TODO thread_v2_id -- might be useful for some other processing?
    delkeys(j, keys=[
        ## messages db
        'user',  # eh. super volatile fields inside it... even full name changes all the time for no reason?
        'is_replied_to_msg_taken_down',
        'hscroll_share',  # some reaction bullshit
        'account_badges',
        ##

        ## threads db
        'recipients',  # same as 'user' in messages db.. pretty volatile
        'has_older_thread_messages_on_server',
        'interop_user_type',
        'transparency_product_enabled',
        'notification_preview_controls',
        'thread_context_items',  # some volatile follower counts?
        'snippet',
        'theme',
        'ig_thread_capabilities',
        'ai_agent_social_signal_message_count',
        'has_groups_xac_ineligible_user',
        ##

        'is_group_xac_calling_eligible',
        'processed_business_suggestion',

        'url_expiration_timestamp_us',
        'is_eligible_for_igd_stacks',
        'profile_pic_url',  # volatile
        'all_media_count',
        'displayed_action_button_type',
        'is_epd',
        'liked_clips_count',
        'reel_media_seen_timestamp',
        'latest_besties_reel_media',
        'latest_fanclub_reel_media',
        'latest_reel_media',

        'follow_friction_type',
        'playable_url_info',
        'preview_url_info',
        'muting',
        'biz_thread_throttling_state',
        'badge_count',
        'follower_count',
        'following_count',

        'last_seen_at',

        'client_context',  # seems to be same as client_item_id -- volatile

        'feed_post_reshare_disabled',

        'is_sent_by_viewer',  # very volatile for no reason??

        'followed_by',
        'account_type',  # sometimes changes between 1 and 2?
        'fan_club_info',  # seems like page description

        'is_business',
        'is_following_current_user',
        'is_interest_account',
        'wa_addressable',

        'inviter',  # thread inviter? volatile

        # seems like fields in it appear and disappear for no reason without any actual status changes
        'friendship_status',

        'hide_in_thread',
        'forward_score',

        ## I think these are properties of messages.user json blob
        'paid_partnership_info',
        'biz_user_inbox_state',
        'has_exclusive_feed_content',
        'has_encrypted_backup',
        'is_using_unified_inbox_for_direct',
        'personal_account_ads_page_id',
        'personal_account_ads_page_name',
        'show_account_transparency_details',
        'organic_tracking_token',
        'should_show_category',
        'fundraiser_tag',
        ##

        'unseen_count',
        'send_attribution',
        'send_silently',
        'smart_suggestion',
        'idempotence_token',

        ## threads.recipients properties
        'can_coauthor_posts',
        'can_coauthor_posts_with_music',
        ##

        'visual_messages_newest_cursor',
        'thread_messages_oldest_cursor',
    ])  # fmt: skip
    j = patch_atoms(j, patch=_patch_volatile_urls)
    return json.dumps(j, sort_keys=True).encode('utf8')


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        msgs = tables['messages']
        assert 'timestamp' in msgs
        assert 'text' in msgs

        _threads = tables['threads']

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop('session')  # super volatile

        for tbl in ['messages', 'threads']:
            t.drop_cols(
                tbl,
                cols=[
                    # changes all the time without changing content
                    '_id',
                    #
                    # kinda volatile, seems to change some time after it's inserted?
                    # doesn't seem used in any indexes etc
                    'client_item_id',
                ],
            )

        t.drop_cols('threads', cols=['last_activity_time'])

        # so message/thread_info tables also contain a json field with raw data, and it's very volatile
        # to clean it up, tried using this at first:
        # SELECT _id, message_type, message, json_remove(message, (SELECT DISTINCT(fullkey) FROM messages, json_tree(message) WHERE atom LIKE '%cdninstagram%')) FROM messages ORDER BY message_type
        # it was promising, but it seems that it's not possible to pass multiple arguments from a scalar subquery
        # it only ended up removing the first key
        c.create_function("CLEANUP_JSONS", 1, _cleanup_jsons)
        queries = [
            'UPDATE messages SET message     = CLEANUP_JSONS(message)',
            'UPDATE threads  SET thread_info = CLEANUP_JSONS(thread_info)',
        ]
        for query in queries:
            list(c.execute(query))
        # a bit insane and experimental... but worked surprisingly smoothly and fast?
        ##


if __name__ == '__main__':
    Normaliser.main()
