#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = {
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
        ('payment_background', 'media_key'),
        ('message_broadcast_ephemeral', 'shared_secret'),

        # the only interesting ones perhaps? checked manually and it's dumped as hex or something, so should be good
        ('audio_data', 'waveform'),
        ('messages', 'raw_data'),  # this one is mostly NULL except one row??
    }

    #def check(self, c) -> None:
    #    tables = Tool(c).get_tables()
    #    assert 'Feeds' in tables, tables
    #    eps = tables['FeedItems']
    #    assert 'link' in eps
    #    assert 'read' in eps

    #    # should be safe to use multiway because of these vvv
    #    media = tables['FeedMedia']
    #    assert 'played_duration'  in media
    #    assert 'last_played_time' in media


    #def cleanup(self, c) -> None:
    #    self.check(c)

    #    t = Tool(c)
    #    # often changing, no point keeping
    #    t.drop_cols(table='Feeds', cols=[
    #        'last_update',
    #    ])


if __name__ == '__main__':
    Normaliser.main()

