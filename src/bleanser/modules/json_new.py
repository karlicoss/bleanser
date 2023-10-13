#!/usr/bin/env python3
from bleanser.core.modules.json_new import *  # noqa: F403, F401

import warnings
warnings.warn("Module 'bleanser.modules.json_new' is deprecated. Use 'bleanser.core.modules.json_new' instead.", DeprecationWarning)


if __name__ == '__main__':
    JsonNormaliser.main()  # noqa: F405
