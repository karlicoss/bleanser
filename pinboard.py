#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter, pipe


class PinboardNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, logger_tag='pinboard-normaliser', delete_dominated=False, keep_both=True) # type: ignore
        # hm, for pinboard might be useful to know when exactly we deleted the bookmark

    def cleanup(self) -> Filter:
        # TODO return hashes etc?
        # return 'sort_by(.time)' #  | map(map_values(ascii_downcase))'
        return '.'

    def extract(self) -> Filter:
        return pipe(
            '.tags  |= .',
            '.posts |= map({href, description, time, tags})', # TODO maybe just delete hash?
            '.notes |= {notes: .notes | map({id, title, updated_at}), count}',  # TODO hhmm, it keeps length but not content?? odd.
       )


def main():
    norm = PinboardNormaliser()
    norm.main()


if __name__ == '__main__':
    main()
