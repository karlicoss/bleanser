import warnings

from bleanser.core.modules.xml import *

warnings.warn(
    "Module 'bleanser.modules.xml_clean' is deprecated. Use 'bleanser.core.modules.xml_clean' instead.",
    DeprecationWarning,
    stacklevel=2,
)


if __name__ == '__main__':
    Normaliser.main()  # noqa: F405
