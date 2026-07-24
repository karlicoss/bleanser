from sqlite3 import Connection

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # HPI-level validation confirmed that multiway retains every extracted place in the observed export history.
    PRUNE_DOMINATED = True
    MULTIWAY = True

    def cleanup(self, c: Connection) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        # This is the content table; its opaque protobufs include saved places, lists, labels, and notes.
        items = tables['sync_item_data']

        # Check the known payload schema while retaining any new columns Google might add.
        expected_columns = {
            'corpus',
            'client_id',
            'server_id',
            'timestamp',
            'feature_fprint',
            'latitude_e6',
            'longitude_e6',
            'numerical_index',
            'string_index',
            'sync_state',
            'item_proto',
        }
        assert expected_columns <= items.keys(), items
        assert items['item_proto'] == 'BLOB', items

        # These tables contain locale and sync transport state rather than Maps content.
        tool.drop(
            'android_metadata',
            'sync_corpus_metadata',
            'sync_metadata',
        )


if __name__ == '__main__':
    Normaliser.main()
