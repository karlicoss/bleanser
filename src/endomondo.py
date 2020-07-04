#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter


class EndoNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, logger_tag='endomondo-normaliser', delete_dominated=True, keep_both=False) # type: ignore

    def cleanup(self) -> Filter:
        return '.'
        # return 'map(.episodes |= map_values(.title |= ascii_downcase))'


def main():
    norm = EndoNormaliser()
    norm.main()


if __name__ == '__main__':
    main()
