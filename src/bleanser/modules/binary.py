"""
Format-agnostic, clean up as literal file diffs
"""
# TODO probably should give it a better name...
# TODO move it to core?

from bleanser.core.processor import BaseNormaliser


class Normaliser(BaseNormaliser):
    # TODO need to be careful about using it...
    # for non-structured data might mess it up by accident if it's weirdly ordered
    pass


if __name__ == '__main__':
    Normaliser.main()
