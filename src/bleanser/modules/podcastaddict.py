#!/usr/bin/env python3
from pathlib import Path
from sqlite3 import Connection
from typing import Optional


from bleanser.core.common import Relation, logger, relations_to_instructions
from bleanser.core.utils import get_tables
from bleanser.core.sqlite import relations, SqliteNormaliser, sqlite_process


class Normaliser(SqliteNormaliser):
    DELETE_DOMINATED = True

    def __init__(self, db: Path) -> None:
        # todo not sure about this?.. also makes sense to run checked for cleanup/extract?
        with self.checked(db) as conn:
            self.tables = get_tables(conn)
        assert 'podcasts' in self.tables, self.tables
        assert 'episodes' in self.tables, self.tables

    def cleanup(self, c: Connection) -> None:
        ## often changing
        c.execute('UPDATE episodes SET thumbnail_id=-1')
        c.execute('UPDATE podcasts SET update_date=-1,episodesNb=-1,thumbnail_id=-1,subscribers=-1')
        ##
        c.execute('DROP TABLE IF EXISTS ordered_list')  # just some random numbers, always changing
        c.execute('DROP TABLE IF EXISTS blocking_services')
        c.execute('DROP TABLE IF EXISTS ad_campaign')
        c.execute('DROP TABLE IF EXISTS topics')
        c.execute('DROP TABLE IF EXISTS sqlite_stat1')  # ???


import click
@click.command()
@click.argument('path', type=Path)
@click.option('--max-workers', required=False, type=int, help='Passed down to ThreadPoolExecutore. Use 0 for serial execution')
def main(path: Path, max_workers: Optional[int]) -> None:
    # TODO collect all sqlite mimes?
    paths = list(sorted(path.rglob('*.db')))
    sqlite_process(paths, Normaliser=Normaliser, max_workers=max_workers)


if __name__ == '__main__':
    main()
