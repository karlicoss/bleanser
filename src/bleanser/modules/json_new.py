import warnings

from bleanser.core.modules.json import *  # noqa: F403, F401

warnings.warn("Module 'bleanser.modules.json_new' is deprecated. Use 'bleanser.core.modules.json' instead.", DeprecationWarning)


if __name__ == '__main__':
    JsonNormaliser.main()  # noqa: F405
