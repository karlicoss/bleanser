#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter, pipe


class PinboardNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, logger_tag='pinboard-normaliser', delete_dominated=False, keep_both=True) # type: ignore
        # hm, for pinboard might be useful to know when exactly we deleted the bookmark

    def cleanup(self) -> Filter:
        return 'sort_by(.time)' #  | map(map_values(ascii_downcase))'

    def extract(self) -> Filter:
        return pipe(
            'sort_by(.time)',
            'map({href})',
        )


def main():
    norm = PinboardNormaliser()
    norm.main()


if __name__ == '__main__':
    main()
