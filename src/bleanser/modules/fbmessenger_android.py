#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # TODO
    # MULTIWAY = True
    # PRUNE_DOMINATED = True

    # msys_database
    ALLOWED_BLOBS = {
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
    }

    DROP_VIRTUAL_TABLES = True

    def check(self, c) -> None:
        return
        tables = Tool(c).get_tables()
        assert 'Feeds' in tables, tables
        eps = tables['FeedItems']
        assert 'link' in eps
        assert 'read' in eps

        # should be safe to use multiway because of these vvv
        media = tables['FeedMedia']
        assert 'played_duration'  in media
        assert 'last_played_time' in media


    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop('secure_message_server_time_v2')
        # eh..some technical information
        t.drop('sync_groups')
        t.drop('orca_upgrade_cql_schema_facets')
        t.drop('secure_message_ab_props_v2') # just weird single value


if __name__ == '__main__':
    Normaliser.main()
