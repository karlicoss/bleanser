from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # multiway is useful at the very least for old db format, it only kept rolling 6K points or something in the db
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        info_tables = [x for x in tables if x.endswith('_info')]
        if len(info_tables) == 0:
            # old db format
            data = tables['data']
            assert 'Time' in data, data
            assert 'Temperature' in data, data
        else:
            # TODO hmm how to add some proper check here without too much duplication?
            pass

    def cleanup(self, c) -> None:
        self.check(c)
        tool = Tool(c)

        tables = tool.get_tables()
        info_tables = [x for x in tables if x.endswith('_info')]
        if len(info_tables) == 0:
            # old db format
            # log_index doesn't correspond to anything real, there are timestamps
            tool.drop_cols(table='data', cols=['log_index'])
            # changes every time db is exported, no point
            tool.drop_cols(table='info', cols=['last_download', 'last_pointer'])
        else:
            for info_table in info_tables:
                # possible to have multiple info tables, e.g. if you have multiple devices

                device, _ = info_table.split('_')

                ## get rid of downloadUnix -- it's changing after export and redundant info
                [[ut]] = list(c.execute(f'SELECT downloadUnix FROM {device}_info'))
                last_logs = [t for t in tables if t.endswith('log')]
                if len(last_logs) == 0:
                    # seems like no data yet
                    return
                last_log = max(last_logs)
                if last_log == f'{device}_{ut}_log':
                    # TODO annoying that it needs to be defensive...
                    # for some dbs it actually does happen, e.g. around 20211102085345
                    tool.drop_cols(table=f'{device}_info', cols=['downloadUnix'])


if __name__ == '__main__':
    Normaliser.main()


# TODO think I've had jdoe or something with example databases..
def test_bluemaestro() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2

    res = actions2(path=TESTDATA / 'bluemaestro', rglob='**/*.db*', Normaliser=Normaliser)

    assert res.remaining == [
        '20180720.db',
        # '20180724.db',  # move
        '20180728.db',
        # '20180730.db',  # move
        '20180731.db',

        '20190723100032.db',  # keep, everything changed
        # TODO need to investigate, some values have changed a bit, like 1st digit after decimal point
        # even timestamps changed sometimes (e.g. just last second)
        # hpi bluemaestro module has something for handling this, I think
        '20190724101707.db',
        # same as above
        '20190727104723.db',

        '20200208225936.db',  # keep, everything changed (several months diff)
        # '20201209083427/bmgateway.db',  # move, completely dominated by the next
        # '20210131102917/bmgateway.db',  # move, completely dominated by the next
        # '20210207183947/bmgateway.db',  # move, completely dominated by the next
        '20210216211844/bmgateway.db',  # keep, errored because couldn't find last _log item
        '20211103234924/bmgateway.db',  # same, previous errored
        '20211106191208/bmgateway.db',
    ]  # fmt: skip
