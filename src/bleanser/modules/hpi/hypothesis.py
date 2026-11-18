from pathlib import Path
from typing import Any, Iterator

from bleanser.core.modules.extract import ExtractObjectsNormaliser

from my.core.cfg import tmp_config
import my.hypothesis


# FIXME need to disable cachew when using normalising via HPI
# otherwise will mess up the cache all the time
# or even potentially can give inconsistent results if there is a bug in cache key


class Normaliser(ExtractObjectsNormaliser):
    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class hypothesis:
                export_path = path

        with tmp_config(modules=my.hypothesis.__name__, config=config):
            ## sanity check to make sure tmp_config worked as expected
            # for most modules should be able to use module.inputs() directly though
            dal = my.hypothesis._dal()
            assert len(dal.sources) == 1
            ##
            yield from my.hypothesis.highlights()


if __name__ == '__main__':
    Normaliser.main()
