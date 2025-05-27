from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = False  # could use it, but no need really?
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        assert 'noise' in tables, tables
        assert 'records' in tables, tables

    def cleanup(self, c) -> None:
        self.check(c)

        # if not finished it's gonna constantly change
        res = c.execute('DELETE FROM records WHERE finished = 0')
        assert res.rowcount <= 1, res.rowcount


if __name__ == '__main__':
    Normaliser.main()
