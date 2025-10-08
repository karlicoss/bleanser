# NOTE: this is experimental for now, best to use the corresponding module bleanser.modules.* instead
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from my.core.cfg import tmp_config

from bleanser.core.modules.extract import ExtractObjectsNormaliser

## disable cache, otherwise it's gonna flush it all the time
# TODO this should be in some sort of common module
os.environ["CACHEW_DISABLE"] = "*"
os.environ.pop("ENLIGHTEN_ENABLE", None)
os.environ["LOGGING_LEVEL_my_fbmessenger_android"] = "WARNING"  # noqa: SIM112
##

import my.fbmessenger.android as module


class Normaliser(ExtractObjectsNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class fbmessenger:
                class android:
                    export_path = path
                    # TODO facebook_id??

        with tmp_config(modules=module.__name__, config=config):
            assert len(module.inputs()) == 1  # sanity check to make sure tmp_config worked as expected
            for m in module.messages():
                yield "message", m
            for c in module.contacts():
                yield "contact", c


if __name__ == "__main__":
    Normaliser.main()
