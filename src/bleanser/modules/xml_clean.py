import warnings

warnings.warn(
    "Module 'bleanser.modules.xml_clean' is deprecated. Use 'bleanser.core.modules.xml_clean' instead.",
    DeprecationWarning,
    stacklevel=2,
)

from typing import TYPE_CHECKING

if not TYPE_CHECKING:
    from bleanser.core.modules.xml import *

    if __name__ == '__main__':
        Normaliser.main()  # noqa: F405
