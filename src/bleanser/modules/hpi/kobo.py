from pathlib import Path
from typing import Any, Iterator

from bleanser.core.modules.extract import ExtractObjectsNormaliser

from my.core.cfg import tmp_config
import my.kobo


class Normaliser(ExtractObjectsNormaliser):
    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class kobo:
                export_path = path

        with tmp_config(modules=my.kobo.__name__, config=config):
            assert len(my.kobo.DATABASES) == 1
            yield from []

            yield from my.kobo._iter_highlights()
            # iter_highlights
            # iter_events
            #
            ## sanity check to make sure tmp_config worked as expected
            # for most modules should be able to use module.inputs() directly though
            # dal = my.hypothesis._dal()
            # assert len(dal.sources) == 1
            ##
            # yield from my.hypothesis.highlights()


if __name__ == '__main__':
    Normaliser.main()
