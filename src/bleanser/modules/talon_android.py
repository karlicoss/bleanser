from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        _tables = Tool(c).get_tables()
        # TODO add something later

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        # for some reason flaking between en/en_US
        t.drop('android_metadata')


if __name__ == '__main__':
    Normaliser.main()
