from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    ALLOWED_BLOBS = frozenset({
        ('downloads', 'hash'),
        ('typed_url_sync_metadata', 'value'),
    })  # fmt: skip

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        # fmt: off
        v = tables['visits']
        assert 'visit_time' in v, v
        assert 'url'        in v, v  # note: url is an int id

        u = tables['urls']
        assert 'url'   in u, u
        assert 'title' in u, u
        # fmt: on

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop_cols(
            'urls',
            cols=[
                # TODO similar issue to firefox -- titles sometimes jump because of notifications (e.g. twitter)
                # maybe could sanitize it?
                # cleans up like 15% databases if I wipe it completely?
                # the annoying thing is that sqlite doesn't have support for regex...
                # 'title',
                #
                # aggregates, no need for them
                'visit_count',
                'typed_count',
                'last_visit_time',
            ],
        )
        t.drop_cols(
            'segment_usage',
            cols=['visit_count'],
        )
        c.execute('DELETE FROM meta WHERE key IN ("typed_url_model_type_state", "early_expiration_threshold")')

        # hmm, not sure -- it might change?
        # cleans up about 10% files
        # t.drop_cols(
        #     'visits',
        #     cols=['visit_duration'],
        # )


if __name__ == '__main__':
    Normaliser.main()
