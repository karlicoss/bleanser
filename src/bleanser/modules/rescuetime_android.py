from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        events = tables['LoggedEvent']
        assert 'started' in events, events
        assert 'appName' in events, events

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop('ScanningPause')  # not sure what is it, but seems to be some sort of helper table
        t.drop('SentryLogEntry')  # some internal logging, contributes to tons of changes
        # todo there is also TimeLog, but it seems that they are also write only and consistent so don't impact diffs


if __name__ == '__main__':
    Normaliser.main()
