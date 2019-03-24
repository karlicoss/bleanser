#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter


ID_FILTER = '.'


class MyshowsNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, logger_tag='myshows-normaliser', delete_dominated=True, keep_both=False)

    def extract(self) -> Filter:
        return 'map(.episodes |= map_values(.title |= ascii_downcase))'
        # return ID_FILTER
        # return 'map(map_)'
        # pass
        # return 'sort_by(.date) | map(map_values(ascii_downcase))'

    def cleanup(self) -> Filter:
        raise NotImplementedError


def main():
    bdir = Path('myshows')
    backups = list(sorted(bdir.glob('*.json')))

    norm = MyshowsNormaliser()
    norm.main(all_files=backups)


if __name__ == '__main__':
    main()
