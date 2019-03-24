#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter, pipe, jdel as d, jq_del_all


def _normalise_coordinates():
    return [
        # TODO shit. take - into account??
        '(.. | .lat?) |= (tostring | .[0:4])',
        '(.. | .lng?) |= (tostring | .[0:4])',
    ]



class FsqNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, logger_tag='fsq-normaliser', delete_dominated=True, keep_both=False)

    # ok, this on only can delete items or do trivial rewrites
    # if we map we might lose data here!
    def cleanup(self) -> Filter:
        return pipe(
            # TODO remove venue stats
            d('.[] | (.meta, .notifications)'),

            # TODO wtf? can I merge then in single one?
            d('.. | .venue?'),
            d('.. | .likes?'),
            # d('.. | .venue? | .url '),
            # d('.. | .venue? | .url '),

            jq_del_all(
                'sticker',
            ),
            jq_del_all(
                'contact',
            ),
            jq_del_all(
                'editableUntil',
                'prefix',
                'consumerId',
            ),
            *_normalise_coordinates(),
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
            'map({id})', #  venue: .venue.name })', just keep venue id??
            *_normalise_coordinates(),
            # TODO not sure if we need to sort?
        )



def main():
    bdir = Path('fsq')
    backups = list(sorted(bdir.glob('*.json')))

    norm = FsqNormaliser()
    norm.main(all_files=backups)


if __name__ == '__main__':
    main()
