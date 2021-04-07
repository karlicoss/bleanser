#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection

from bleanser.core.common import logger
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import SqliteNormaliser


class Normaliser(SqliteNormaliser):
    DELETE_DOMINATED = True

    def __init__(self, db: Path) -> None:
        # todo not sure about this?.. also makes sense to run checked for cleanup/extract?
        with self.checked(db) as conn:
            self.tables = get_tables(conn)
            [info_table] = (x for x in self.tables if x.endswith('_info'))
            self.device, _ = info_table.split('_')

    def cleanup(self, c: Connection) -> None:
        D = self.device
        ## get rid of downloadUnix -- it's changing after export and redundant info
        [[ut]] = list(c.execute(f'SELECT downloadUnix FROM {D}_info'))
        last_log = max(t for t in self.tables if t.endswith('log'))
        assert last_log == f'{D}_{ut}_log', last_log
        c.execute(f'UPDATE {D}_info set downloadUnix=-1')


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=Normaliser, glob='*.db')
