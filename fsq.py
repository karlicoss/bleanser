#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter, pipe, jdel as d, jq_del_all


class FsqNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, logger_tag='fsq-normaliser', delete_dominated=True, keep_both=False)

    # ok, this on only can delete items or do trivial rewrites
    # if we map we might lose data here!
    def cleanup(self) -> Filter:
        return pipe(
            # TODO remove venue stats
            d('.[] | (.meta, .notifications)'),
            d('.. | .venue? | .stats '),
            jq_del_all(
                'sticker',
            ),
            jq_del_all(
                'contact',
            ),
            '(.. | .lat?) |= (tostring | .[0:5])',
            '(.. | .lng?) |= (tostring | .[0:5])',
            # TODO shit. again, we want to assert...
            # 'map(.response)',
            # 'map(.checkins)',
        )
    # TODO shit. lat and lng jump randomly.. can we trim them?
        # return '.'
        # return 'sort_by(.date) | map(map_values(ascii_downcase))'

    def extract(self) -> Filter:
        return pipe(
            'map_values(.response)',
            'map_values(.checkins)',
            'map_values(.items)',
            '.[]',
            # '.[]',
            'map({id})',
            # TODO not sure if we need to sort?
        )



def main():
    bdir = Path('fsq')
    backups = list(sorted(bdir.glob('*.json')))

    norm = FsqNormaliser()
    norm.main(all_files=backups)


if __name__ == '__main__':
    main()
