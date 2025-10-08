import warnings

from bleanser.core.modules.json import *

warnings.warn(
    "Module 'bleanser.modules.json_new' is deprecated. Use 'bleanser.core.modules.json' instead.",
    DeprecationWarning,
    stacklevel=2,
)


if __name__ == '__main__':
    JsonNormaliser.main()  # noqa: F405
