from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = frozenset({
        ('msg', 'content_pb'),
        ('im_search_index_official_segments', '*'),
        ('im_search_index_official_segdir', '*'),
        ('im_search_index_official_docsize', '*'),
        ('im_search_index_official_stat', '*'),
    })  # fmt: skip

    def check(self, c) -> None:
        tables = Tool(c).get_tables()

        messages = tables['msg']
        assert 'msg_uuid' in messages
        assert 'content' in messages

    def cleanup(self, c) -> None:
        self.check(c)


if __name__ == '__main__':
    Normaliser.main()
