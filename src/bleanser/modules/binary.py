#!/usr/bin/env python3
"""
Format-agnostic, clean up as literal file diffs
"""
# TODO probably should give it a better name...

from bleanser.core.processor import BaseNormaliser


class Normaliser(BaseNormaliser):
    # filter out additions; keep the rest
    DIFF_FILTER =  '> '


if __name__ == '__main__':
    Normaliser.main()
