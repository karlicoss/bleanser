import warnings

warnings.warn(
    "Module 'bleanser.modules.json_new' is deprecated. Use 'bleanser.core.modules.json' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from bleanser.core.modules.json import *

    if __name__ == '__main__':
        JsonNormaliser.main()  # noqa: F405
