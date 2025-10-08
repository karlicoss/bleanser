from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = frozenset({
        ('messages_fts_segments', 'block'),
        ('messages_fts_segdir', 'root'),
        ('message_ftsv2_segments', 'block'),
        ('message_ftsv2_segdir', 'root'),
        ('message_ftsv2_docsize', 'size'),
        ('message_ftsv2_stat', 'value'),
        ('messages_quotes', 'raw_data'),
        ('message_quoted_location', 'thumbnail'),
        ('message_media', 'media_key'),
        ('message_media', 'first_scan_sidecar'),
        ('message_quoted_media', 'media_key'),
        ('message_quoted_media', 'thumbnail'),
        ('message_thumbnails', 'thumbnail'),
        ('message_streaming_sidecar', 'sidecar'),
        ('message_streaming_sidecar', 'chunk_lengths'),
        ('pay_transactions', 'future_data'),
        ('pay_transaction', 'future_data'),
        ('message_thumbnail', 'thumbnail'),
        ('media_hash_thumbnail', 'thumbnail'),
        ('message_quoted_text', 'thumbnail'),
        ('message_future', 'data'),
        ('message_system_photo_change', 'old_photo'),
        ('message_system_photo_change', 'new_photo'),
        ('labeled_messages_fts_segments', 'block'),
        ('labeled_messages_fts_segdir', 'root'),
        ('message_external_ad_content', 'full_thumbnail'),
        ('message_external_ad_content', 'micro_thumbnail'),
        ('message_order', 'thumbnail'),
        ('message_quoted_order', 'thumbnail'),
        ('mms_thumbnail_metadata', 'media_key'),
        ('mms_thumbnail_metadata', 'micro_thumbnail'),
        ('message_invoice', 'attachment_media_key'),
        ('message_invoice', 'attachment_file_sha256'),
        ('message_invoice', 'attachment_file_enc_sha256'),
        ('message_invoice', 'attachment_jpeg_thumbnail'),
        ('message_quote_invoice', 'attachment_jpeg_thumbnail'),
        ('quoted_message_order', 'thumbnail'),
        ('message_add_on_orphan', 'orphan_message_data'),
        ('message_orphaned_edit', 'orphan_message_data'),
        ('payment_background', 'media_key'),
        ('message_broadcast_ephemeral', 'shared_secret'),
        ('message_secret', 'message_secret'),
        ('message_poll', 'enc_key'),
        ('suggested_replies', 'customer_message_embedding'),
        ('smart_suggestions_key_value', '*'),
        ('message_future', 'future_proof_stanza'),
        ('addon_message_media', 'scans_sidecar'),
        ('addon_message_media', 'media_key'),
        ('bot_plugin_metadata', '*'),
        ('message_orphan', '*'),  # not sure what is it but seems empty
        ('bcall_session', 'master_key'),
        #
        # the only interesting ones perhaps? checked manually and it's dumped as hex or something, so should be good
        ('audio_data', 'waveform'),
        ('messages', 'raw_data'),  # this one is mostly NULL except one row??
    })  # fmt: skip

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        chat = tables['chat']
        assert 'subject' in chat
        assert 'created_timestamp' in chat

        if 'messages' in tables:
            msgs = tables['messages']
            assert 'data' in msgs
        else:
            msgs = tables['message']  # new format (at least as of Nov 2022)
            assert 'text_data' in msgs

        assert 'timestamp' in msgs

        # maybe also
        # audio_data
        # call_log (call_id, timestamp)
        # chat

        # TODO group_participant_user
        # TODO message_media not sure if useful?

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)

        # note: there are WAY more useless tables there, but for the most part they are empty
        for table in [
            'frequent',
            'frequents',  # freq used contacts
            'group_notification_version',
            'group_participant_device',  # not sure who'd need it
            'media_hash_thumbnail',
            'media_refs', # just random file paths with counters
            'message_forwarded', # keeps track of some forward_score??

            ## some sort of search index
            'message_ftsv2',
            'message_ftsv2_content',
            'message_ftsv2_docsize',
            'message_ftsv2_segdir',
            'message_ftsv2_segments',
            'message_ftsv2_stat',
            ##

            'message_streaming_sidecar', #  some random numbers
            'message_thumbnail',
            'message_thumbnails',
            'message_ui_elements',
            'message_ui_elements_reply',
            'message_vcard',
            'message_vcard_jid',
            'message_view_once_media',
            'messages',

            ## also some search index
            'messages_fts',
            'messages_fts_content',
            'messages_fts_segdir',
            'messages_fts_segments',
            ##

            'messages_links',
            'messages_quotes',
            'messages_vcards',
            'messages_vcards_jids',
            'mms_thumbnail_metadata',
            'primary_device_version',  # just some random numbers??
            'props',  # some random metadata, changes all the time
            'receipt_device',
            'receipt_orphaned',
            'receipt_user',
            'receipts',  # not sure why would it be useful to keep track of
            'status',  # keeps track of last read msg or something
            'user_device',
            'user_device_info',

            ### from newer app versions
            'message_details', # some crap like author_device_jid
            ## some sort of aggregates or indices into stuff like emoji? wtf...
            'message_add_on',
            'message_add_on_reaction',
            'message_add_on_receipt_device',
            ##

            'message_system', # random numbers
            'message_system_chat_participant',

            'message_template',
            'message_template_button',
            'message_send_count',

            'backup_changes',  # some internal state handling, doesn't have anything useful
            'message_system_initial_privacy_provider',

            'message_link',  # seems like an index of urls in messages
            'message_status_psa_campaign',

            'deleted_chat_job',
            'scheduled_reminder_message',
            'message_system_photo_change',
            'status_crossposting_v3',

            # TODO group_past_participant_user not sure if useful? kinda volatile
        ]:  # fmt: skip
            t.drop(table)

        t.drop_cols(
            table='chat',
            cols=[
                '_id',  # actual chat id seems to be in jid_row_id
                'hidden',  # kinda volatile and not that interesting to track
                'display_message_row_id',
                'last_message_row_id',
                'last_read_message_row_id',
                'last_read_receipt_sent_message_row_id',
                'last_important_message_row_id',
                'sort_timestamp',
                'spam_detection',
                'unseen_earliest_message_received_time',
                'unseen_message_count',
                'unseen_row_count',
                'unseen_message_reaction_count',
                'unseen_important_message_count',
                'history_sync_progress',
                'change_number_notified_message_row_id',
                ## newer db versions
                # flaky fields
                'last_read_message_sort_id',
                'display_message_sort_id',
                'last_message_sort_id',
                'last_read_receipt_sent_message_sort_id',
                'last_message_reaction_row_id',
                'last_seen_message_reaction_row_id',
                'mod_tag',
                'has_new_community_admin_dialog_been_acknowled',
                'show_group_description',
                'growth_lock_level',
                'growth_lock_expiration_ts',
                'chat_origin',
                # NOTE ugh. created_timestamp is a bit flaky? often goes from NULL to the actual value
                # this might be an interesting usecase/test for extract mode?
                'created_timestamp',
                ## for 'extract' mode
                ## archived?
            ],
        )

        t.drop_cols(
            table='message',
            cols=[
                ## flaky, no idea what is it
                'origination_flags',
                'message_add_on_flags',
                'status',
                ##
                ## for 'extract' mode:
                # received_timestamp
                # receipt_server_timestamp
            ],
        )

        # if it's not transferred yet gonna be volatile (e.g. size, no path etc)
        # so best to just ignore it
        c.execute('DELETE FROM message_media WHERE transferred != 1')

        t.drop_cols(
            table='message_media',
            cols=[
                'original_file_hash',  # ??? sometimes goes from value to NULL
                ## flaky 0/1
                'has_streaming_sidecar',
                'autotransfer_retry_enabled',
                'transferred',
                'transcoded',
                ##
                # todo media_name might be flaky?? sometimes sets from NULL to file name?
                # same info is in file_path though... so idk
            ],
        )

        # the mapping itself is between group_jid_row_id and user_jid_row_id columns
        # seems like sometimes entries are reordered or something
        for table in [
            'group_participant_user',
            'group_past_participant_user',
            # TODO not sure if should do same with message and chat tables?
        ]:
            t.drop_cols(
                table=table,
                cols=[
                    '_id',
                    'rank',
                ],
            )


if __name__ == '__main__':
    Normaliser.main()


# TODO message_quoted could be kinda useful? not sure if anything else contains reference to the original message
# message_ephemeral -- expiring messages? not sure if interesting
