#!/usr/bin/env python3
from pathlib import Path

from jq_normaliser import JqNormaliser, Filter


class HypNormaliser(JqNormaliser):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs, logger_tag='hypothesis-normaliser', delete_dominated=True, keep_both=False) # type: ignore

    def cleanup(self) -> Filter:
        return '.'


def main():
    norm = HypNormaliser()
    norm.main()


if __name__ == '__main__':
    main()
