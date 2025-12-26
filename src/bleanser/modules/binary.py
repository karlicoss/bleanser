import warnings

warnings.warn(
    "Module 'bleanser.modules.binary' is deprecated. Use 'bleanser.core.modules.binary' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from bleanser.core.modules.binary import *

    Normaliser = BinaryNormaliser  # noqa: F405  # legacy name

    if __name__ == '__main__':
        BinaryNormaliser.main()  # noqa: F405
