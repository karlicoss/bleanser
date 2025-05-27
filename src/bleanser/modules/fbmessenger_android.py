from __future__ import annotations

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        mtable = tables['messages']
        assert 'text' in mtable, mtable
        assert 'timestamp_ms' in mtable, mtable
        # TODO check that it has anything in there????
        #
        # TODO hmm instead need to check threads database!!! and check that there are some threads inside??

    def cleanup(self, c) -> None:
        t = Tool(c)
        if 'logging_events_v2' in t.get_tables():
            self.cleanup_msys_database(c)
            return

        self.check(c)
        t.drop('properties')  # eh, just some weird random key-values
        t.drop('virtual_folders')  # looks like some temporary thing, just a few values
        t.drop('_shared_version')
        t.drop('folder_counts')

        for name in ['folders', 'threads', 'thread_participants', 'thread_themes', 'thread_users']:
            t.drop_cols(name, cols=['_id'])  # meh.. sometimes changes for no reason

        t.drop_cols('folders', cols=['timestamp_ms'])  # changes all the time
        t.drop_cols(
            'thread_participants',
            cols=[
                'last_read_receipt_time',
                'last_read_receipt_watermark_time',
                'last_delivered_receipt_time',
            ],
        )
        # changes all the time
        t.drop_cols('threads', cols=[
            'sequence_id',
            'last_snippet_update_timestamp_ms',
            'last_message_timestamp_ms',
            'last_message_id',
            'last_fetch_time_ms',
            'last_read_timestamp_ms',
            'timestamp_ms',
            'snippet',
            'admin_snippet',
            'approx_total_message_count',
            'unread_message_count',
            'vanish_mode_selection_timestamp',

            'rtc_room_info',
            'rtc_call_info',

            # todo?
            'snippet_sender',
            'senders',
        ])  # fmt: skip

        t.drop_cols(
            'thread_users',
            cols=[
                'last_fetch_time',
                'aloha_proxy_users_owned',
                'profile_pic_square',
                'contact_capabilities',
                'contact_capabilities2',
            ],
        )

    def cleanup_msys_database(self, c) -> None:
        # TODO eh... tbh, not sure, msys database only contains some contacts and bunch of cryptic data?
        # self.check(c) # TODO

        t = Tool(c)
        tables = t.get_tables()
        t.drop('secure_message_server_time_v2')
        # eh..some technical information
        t.drop('sync_groups')
        t.drop('orca_upgrade_cql_schema_facets')
        t.drop('secure_message_ab_props_v2')  # just weird single value
        t.drop('pending_tasks')  # temporary thing?
        t.drop('crypto_auth_token')
        t.drop('logging_events_v2')

        t.drop('quick_promotion_filters')
        t.drop('quick_promotions')
        t.drop('presence_states')

        ## meh, there is no our own data in these, and changing all the time -- not worth keeping stories
        t.drop('stories')
        t.drop('story_buckets')
        t.drop('story_attribution_ranges')
        t.drop('story_overlays')
        t.drop('story_viewers')
        ##

        t.drop('cm_search_nullstate_metadata')
        t.drop('thread_themes')
        t.drop('mailbox_metadata')
        t.drop('experiment_value')
        t.drop('family_experiences')
        t.drop('logging_modules')
        t.drop('lightspeed_task_context')
        t.drop('secure_message_edge_routing_info_v2')
        t.drop('secure_message_client_state')
        t.drop('secure_message_other_devices')
        t.drop('reaction_v2_types')
        t.drop('pending_task_parents')
        t.drop('hmps_status')
        t.drop('_cached_participant_thread_info')
        t.drop('messenger_encrypted_messaging_periodic_tasks')
        t.drop('push_notifications')
        t.drop('gradient_colors')
        t.drop('messenger_dynamic_presence_backgrounds')
        t.drop('avatar_template_pack_entries')
        t.drop('avatar_template_packs')
        t.drop('messages_ranges_v2__generated')
        t.drop('notif_silent_push_settings')
        t.drop('messaging_privacy_settings')
        t.drop('secure_message_client_identity_v2')
        t.drop('secure_composer_state')
        t.drop('secure_message_signed_pre_keys_v2')
        t.drop('secure_message_client_state_v2')
        t.drop('messages_optimistic_context')
        t.drop('community_messaging_aggregated_copresence_counts_for_chat')
        t.drop('community_thread_sync_info')
        t.drop('inbox_threads_ranges')
        t.drop('sync_group_threads_ranges')
        t.drop('secure_message_pre_keys_v2')
        t.drop('sharing_life_events')
        t.drop('fw_ranking_scores')
        t.drop('fw_ranking_requests')
        t.drop('threads_ranges__generated')
        t.drop('threads_ranges_v2__generated')
        t.drop('community_messaging_aggregated_user_presence_counts_for_community')

        t.drop_cols(
            'participants',
            cols=[
                ## volatile
                'last_message_send_timestamp_ms',
                'read_watermark_timestamp_ms',
                'delivered_watermark_timestamp_ms',
                'read_action_timestamp_ms',
                'capabilities',
                'participant_capabilities',
                ##
            ],
        )

        # TODO move to Tool?
        def drop_cols_containing(tbl_name: str, *, containing: list[str]) -> None:
            tbl = tables.get(tbl_name)
            if tbl is None:
                return
            cols_to_drop = [col for col in tbl if any(s in col for s in containing)]
            t.drop_cols(tbl_name, cols=cols_to_drop)

        drop_cols_containing(
            'threads',
            containing=[
                'thread_picture_url',
                'last_activity_timestamp_ms',
                'last_activity_watermark_timestamp_ms',
                'last_read_watermark_timestamp_ms',
                'last_message_cta_id',
                'reviewed_policy_violation',
                'reported_policy_violation',
                'snippet_text',
                'snippet_sender_contact_id',
            ],
        )
        t.drop_cols(
            'threads',
            cols=[
                'snippet',
                'sort_order_override',
                ## volatile
                'member_count',
                'locked_status',
                'thread_capabilities_fetch_ts',
                'event_start_timestamp_ms',
                'event_end_timestamp_ms',
                'snippet_has_emoji',
                'capabilities',
                'should_round_thread_picture',
                # TODO snipper_sender_contact_id sometimes changes??
                ##
            ],
        )
        t.drop_cols(
            'messages',
            cols=[
                ## volatile
                'authority_level',
                'send_status',
                'send_status_v2',
                ##
            ],
        )

        drop_cols_containing(
            'community_folders',
            containing=[
                'picture_url',
            ],
        )
        t.drop_cols(
            'community_folders',
            cols=[
                ## volatile
                'member_count',
                'capabilities',
                ##
            ],
        )

        t.drop_cols(
            'contacts',
            cols=[
                ## volatile
                'family_relationship',
                'requires_multiway',
                'capabilities',
                'capabilities_2',
                'rank',
                'is_messenger_user',
                'contact_type_exact',
                'messenger_call_log_third_party_id',
                # TODO montage_thread_fbid is volatile?
                ##
            ],
        )
        t.drop_cols(
            'client_contacts',
            cols=[
                'capabilities_1',
                'capabilities_2',
            ],
        )

        # TODO fb_transport_contacts?
        for tbl_name in ['contacts', 'client_contacts', 'client_threads']:
            # these change all the time and expire
            drop_cols_containing(
                tbl_name,
                containing=[
                    'profile_picture',
                    'profile_ring_color',
                    'avatar_animation',
                    'fb_unblocked_since_timestamp_ms',
                    'profile_ring_state',
                    'wa_connect_status',
                    'restriction_type',
                ],
            )

        drop_cols_containing(
            'fb_events',
            containing=[
                'event_picture_url',
                'num_going_users',
                'num_interested_users',
            ],
        )

        drop_cols_containing(
            'attachments',
            containing=[
                'preview_url',
                'playable_url',
            ],
        )


if __name__ == '__main__':
    Normaliser.main()
