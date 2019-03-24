#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter


ID_FILTER = '.'


class LastfmNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, logger_tag='lastfm-normaliser', delete_dominated=True, keep_both=False)

    def extract(self) -> Filter:
        return 'sort_by(.date) | map(map_values(ascii_downcase))'

    def cleanup(self) -> Filter:
        return ID_FILTER


def main():
    bdir = Path('lastfm')
    backups = list(sorted(bdir.glob('*.json')))

    norm = LastfmNormaliser()
    norm.main(all_files=backups)


if __name__ == '__main__':
    main()
