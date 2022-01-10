#!/usr/bin/env python3
from bleanser.core.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # multiway is useful at the very least for old db format, it only kept rolling 6K points or something in the db
    MULTIWAY = True
    DELETE_DOMINATED = True

    def cleanup(self, c) -> None:
        tool = Tool(c)

        tables = tool.get_schemas()
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
            last_log = max(t for t in tables if t.endswith('log'))
            assert last_log == f'{device}_{ut}_log', last_log
            tool.drop_cols(table=f'{device}_info', cols=['downloadUnix'])


if __name__ == '__main__':
    Normaliser.main()


# TODO add tests
# think I've had jdoe or something with example databases..
# BM1 = thedb(bm / '20210225015750')
# BM2 = thedb(bm / '20210228191259')
# BM2 strictly dominates BM1
