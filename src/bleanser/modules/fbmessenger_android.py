#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


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
    }

    DROP_VIRTUAL_TABLES = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        mtable = tables['messages']
        assert 'text'         in mtable, mtable
        assert 'timestamp_ms' in mtable, mtable

    def cleanup(self, c) -> None:
        t = Tool(c)
        if 'logging_events_v2' in t.get_tables():
            self.cleanup_msys_database(c)
            return

        self.check(c)
        t.drop('properties')  # eh, just some weird random key-values
        t.drop('virtual_folders')  # looks like some temporary thing, just a few values

        for name in ['folders', 'threads', 'thread_participants', 'thread_themes']:
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

            # todo?
            'snippet_sender',
            'senders',
        ])


    def cleanup_msys_database(self, c) -> None:
        # TODO eh... tbh, not sure, msys database only contains some contacts and bunch of cryptic data?
        # self.check(c) # TODO

        t = Tool(c)
        t.drop('secure_message_server_time_v2')
        # eh..some technical information
        t.drop('sync_groups')
        t.drop('orca_upgrade_cql_schema_facets')
        t.drop('secure_message_ab_props_v2') # just weird single value
        t.drop('pending_tasks') # temporary thing?
        t.drop('crypto_auth_token')
        t.drop('logging_events_v2')


if __name__ == '__main__':
    Normaliser.main()
