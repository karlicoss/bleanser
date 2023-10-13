#!/usr/bin/env python3
from typing import List

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = {
        # msys_database
          ('secure_message_pre_keys_v2'           , '*') # not useful
        ## mostly empty, dunno what it is
        , ('secure_message_client_identity_v2'    , '*')
        , ('secure_message_session_state_v2'      , '*')
        , ('secure_message_ab_props_v2'           , '*')
        , ('secure_encrypted_backups_client_state', '*')
        , ('advanced_crypto_transport_attachments', '*')
        , ('whats_app_media_item'                 , '*')
        , ('secure_recovery_code_data'            , '*')
        , ('secure_encrypted_backups_epochs'      , '*')
        , ('advanced_crypto_transport_messages'   , '*')
        , ('advanced_crypto_transport_legacy_attachments', '*')
        ##
        , ('secure_message_signed_pre_keys_v2'    , '*')
        , ('secure_message_media_key_v2'          , '*')
        , ('encrypted_backups_client_state'       , '*')

        , ('secure_message_secret_keys', 'value')
        , ('secure_message_sender_key_v2', 'sender_key')
        , ('crypto_auth_token', 'token')
        , ('remote_sp_set', 'remote_sp_blob')
        , ('secure_message_edge_routing_info_v2', 'edge_routing_info')
        , ('secure_message_futureproof_data_v2', 'futureproof_data')
        , ('encrypted_message_futureproof_data', 'futureproof_data')
        , ('messenger_encrypted_messaging_pending_messages', 'application_data')
        , ('secure_message_decrypt_journal_v2', 'journal_data')
        , ('secure_encrypted_backups_epochs', 'epoch_root_key_blob')
        , ('secure_message_icdc_additional_devices_v2', 'remote_identity_key')
        , ('secure_message_icdc_metadata_v2', 'signature_device_key')
        , ('secure_encrypted_backups_devices', 'public_key')
        , ('advanced_crypto_transport_downloaded_attachments', 'plaintext_hash')
        , ('whats_app_contact', 'device_list')
        , ('whats_app_z1_payment_transaction', 'metadata')
        , ('whats_app_group_info', 'extension')
        , ('encrypted_backups_virtual_devices', 'virtual_device_id')
        , ('secure_encrypted_backups_qr_add_device_context', 'temp_ocmf_client_state')
        , ('advanced_crypto_transport_appdata_messages', 'serialized_payload') # used to be in msys db
        , ('messages_optimistic_context', 'dety_params')
        , ('secure_message_poll_secret_v2', 'poll_key')

        , ('secure_acs_configurations', '*')
        , ('secure_acs_blinded_tokens', '*')
        , ('secure_acs_tokens', '*')
        , ('pending_backups_protobufs', 'protobuf_blob')
        , ('local_message_persistence_store_supplemental', 'protobuf')
        , ('local_message_persistence_store', 'protobuf')
        , ('secure_get_secrets_context', 'temp_ocmf_client_state')
        , ('local_message_persistence_store_deleted_messages', 'deleted_message_payload')
        , ('local_message_persistence_store_supplemental', 'message_payload')  # hmm? 
        , ('local_message_persistence_store', 'message_payload') 
        , ('sap_vesta_register_context_v2', '*')
        , ('messenger_encrypted_messaging_stanzas', 'stanza')
    }

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        mtable = tables['messages']
        assert 'text'         in mtable, mtable
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
        t.drop_cols('thread_participants', cols=[
            'last_read_receipt_time',
            'last_read_receipt_watermark_time',
            'last_delivered_receipt_time',
        ])
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
        ])

        t.drop_cols('thread_users', cols=[
            'last_fetch_time',
            'aloha_proxy_users_owned',
            'profile_pic_square',
            'contact_capabilities',
            'contact_capabilities2',
        ])


    def cleanup_msys_database(self, c) -> None:
        # TODO eh... tbh, not sure, msys database only contains some contacts and bunch of cryptic data?
        # self.check(c) # TODO

        t = Tool(c)
        tables = t.get_tables()
        t.drop('secure_message_server_time_v2')
        # eh..some technical information
        t.drop('sync_groups')
        t.drop('orca_upgrade_cql_schema_facets')
        t.drop('secure_message_ab_props_v2') # just weird single value
        t.drop('pending_tasks') # temporary thing?
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


        t.drop_cols('participants', cols=['last_message_send_timestamp_ms'])

        # TODO not sure about these?
        # t.drop('sync_group_threads_ranges')
        # t.drop('threads_ranges__generated')

        # TODO move to Tool?
        def drop_cols_containing(tbl_name: str, *, containing: List[str]) -> None:
            tbl = tables.get(tbl_name)
            if tbl is None:
                return
            cols_to_drop = [col for col in tbl if any(s in col for s in containing)]
            t.drop_cols(tbl_name, cols=cols_to_drop)

        drop_cols_containing('threads', containing=[
            'thread_picture_url',
            'last_activity_timestamp_ms',
            'last_activity_watermark_timestamp_ms',
            'last_read_watermark_timestamp_ms',
            'last_message_cta_id',
        ])
        t.drop_cols('threads', cols=['snippet', 'sort_order_override'])

        for tbl_name in ['contacts', 'client_contacts']:
            # these change all the time and expire
            drop_cols_containing(tbl_name, containing=[
                'profile_picture',
                'profile_ring_color',
                'avatar_animation',
                'fb_unblocked_since_timestamp_ms',
                'profile_ring_state',
                'wa_connect_status',
                'restriction_type',
            ])
            # TODO montage_thread_fbid? seems to change often?

        drop_cols_containing('fb_events', containing=[
            'event_picture_url',
            'num_going_users',
            'num_interested_users',
        ])

        drop_cols_containing('attachments', containing=[
            'preview_url',
            'playable_url',
        ])

if __name__ == '__main__':
    Normaliser.main()
