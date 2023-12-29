# NOTE: this is experimental for now, best to use the corresponding module bleanser.modules.* instead
import os
from pathlib import Path
from typing import Any, Iterator

from bleanser.core.modules.extract import ExtractObjectsNormaliser

from my.core.cfg import tmp_config

## disable cache, otherwise it's gonna flush it all the time
# TODO this should be in some sort of common module
os.environ["CACHEW_DISABLE"] = "*"
os.environ.pop("ENLIGHTEN_ENABLE", None)
os.environ["LOGGING_LEVEL_my_whatsapp_android"] = "WARNING"
##

import my.whatsapp.android as module


class Normaliser(ExtractObjectsNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class whatsapp:
                class android:
                    export_path = path
                    # TODO my_user_id?

        with tmp_config(modules=module.__name__, config=config):
            assert (
                len(module.inputs()) == 1
            )  # sanity check to make sure tmp_config worked as expected
            for m in module.entities():
                yield m


if __name__ == "__main__":
    Normaliser.main()
