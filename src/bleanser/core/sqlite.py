import warnings

from bleanser.core.modules.sqlite import *

warnings.warn(
    "Module 'bleanser.core.sqlite' is deprecated. Use 'bleanser.core.modules.sqlite' instead.",
    DeprecationWarning,
)


if __name__ == '__main__':
    SqliteNormaliser.main()  # noqa: F405
