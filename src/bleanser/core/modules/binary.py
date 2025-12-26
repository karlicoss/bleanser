"""
Format-agnostic, clean up as literal file diffs
"""

from ..processor import BaseNormaliser


class BinaryNormaliser(BaseNormaliser):
    PRUNE_DOMINATED = False
    MULTIWAY = False

    # NOTE it only works properly because we don't ever call diff (it's only used in multiway/prune dominated mode to compute issubset)
    # Otherwise it could be a problem since we diff binary files, at the very least it's not actually going to output a proper diff..
    pass


if __name__ == '__main__':
    BinaryNormaliser.main()
