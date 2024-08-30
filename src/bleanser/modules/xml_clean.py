import warnings

from bleanser.core.modules.xml import *  # noqa: F401, F403

warnings.warn("Module 'bleanser.modules.xml_clean' is deprecated. Use 'bleanser.core.modules.xml_clean' instead.", DeprecationWarning)


if __name__ == '__main__':
    Normaliser.main()  # noqa: F405
