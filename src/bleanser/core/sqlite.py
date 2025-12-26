import warnings

warnings.warn(
    "Module 'bleanser.core.sqlite' is deprecated. Use 'bleanser.core.modules.sqlite' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from bleanser.core.modules.sqlite import *

    if __name__ == '__main__':
        SqliteNormaliser.main()  # noqa: F405
