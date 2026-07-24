# NOTE: this is experimental for now, best to use the corresponding module bleanser.modules.* instead
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from my.core.cfg import tmp_config

from bleanser.core.modules.extract import ExtractObjectsNormaliser

os.environ['CACHEW_DISABLE'] = '*'
os.environ.pop('ENLIGHTEN_ENABLE', None)
os.environ['LOGGING_LEVEL_my_google_maps_android'] = 'WARNING'  # noqa: SIM112

import my.google.maps.android as module


class Normaliser(ExtractObjectsNormaliser):
    # TODO: The generic multi-normaliser consistency assertion can reject this valid multiway result.
    # This normaliser and the SQLite normaliser choose different pivot files despite preserving the same HPI object union.
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class google:
                class maps:
                    class android:
                        export_path = path

        with tmp_config(modules=module.__name__, config=config):
            assert len(module.inputs()) == 1
            yield from module.saved()


if __name__ == '__main__':
    Normaliser.main()
