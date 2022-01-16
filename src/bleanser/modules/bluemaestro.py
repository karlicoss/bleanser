#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # multiway is useful at the very least for old db format, it only kept rolling 6K points or something in the db
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, c) -> None:
        tool = Tool(c)

        tables = tool.get_tables()
        info_tables = [x for x in tables if x.endswith('_info')]
        if len(info_tables) == 0:
            # old db format
            # log_index doesn't correspond to anything real, there are timestamps
            dtable = tables['data']
            assert 'Time' in dtable, dtable
            tool.drop_cols(table='data', cols=['log_index'])
            # changes every time db is exported, no point
            tool.drop_cols(table='info', cols=['last_download', 'last_pointer'])
        else:
            [info_table] = info_tables
            device, _ = info_table.split('_')

            ## get rid of downloadUnix -- it's changing after export and redundant info
            [[ut]] = list(c.execute(f'SELECT downloadUnix FROM {device}_info'))
            last_logs = [t for t in tables if t.endswith('log')]
            if len(last_logs) == 0:
                # seems like no data yet
                return
            last_log = max(last_logs)
            assert last_log == f'{device}_{ut}_log', last_log
            tool.drop_cols(table=f'{device}_info', cols=['downloadUnix'])


if __name__ == '__main__':
    Normaliser.main()


# TODO think I've had jdoe or something with example databases..
def test_bluemaestro() -> None:
    from bleanser.tests.common import skip_if_no_data; skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2
    res = actions2(path=TESTDATA / 'bluemaestro', rglob='**/*.db*', Normaliser=Normaliser)

    assert res.remaining == [
        '20180720.db',
        # '20180724.db',  # move
        '20180728.db',
        # '20180730.db',  # move
        '20180731.db',

        '20190723100032.db', # keep, everything changed
        # TODO need to investigate, some values have changed a bit, like 1st digit after decimal point
        # even timestmaps chagned sometimes (e.g. just last second)
        # hpi bluemaestro module has something for handling this, I think
        '20190724101707.db',
        # same as above
        '20190727104723.db',

        '20200208225936.db', # keep, everything changed (several months diff)
        # '20201209083427/bmgateway.db',  # move, completely dominated by the next
        # '20210131102917/bmgateway.db',  # move, completely dominated by the next
        # '20210207183947/bmgateway.db',  # move, completely dominated by the next
        '20210216211844/bmgateway.db',  # keep, errored because couldn't find last _log item
        '20211103234924/bmgateway.db',  # same, previous errored
        '20211106191208/bmgateway.db',
    ]
