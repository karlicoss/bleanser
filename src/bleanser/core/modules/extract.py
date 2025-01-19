from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from bleanser.core.processor import (
    BaseNormaliser,
    Normalised,
    sort_file,
    unique_file_in_tempdir,
)


class ExtractObjectsNormaliser(BaseNormaliser):
    """
    This is meant to be overridden by a subclass

    extract_objects receives an input file, and should yield data/objects that when converted
    to a string, produces some comparable data/object to the normalised/cleaned output file

    possible things this could return is a unique key/id, or a tuple of (key, data), or a
    namedtuple/dataclass

    newlines are stripped from the string, so lines can be compared/diffed properly

    Its possible you could use a library or code from https://github.com/karlicoss/HPI
    in extract_objects, to use the DAL itself to parse the file https://beepb00p.xyz/exports.html#dal
    """

    def extract_objects(self, path: Path) -> Iterator[Any]:
        raise NotImplementedError
        # when you subclass, you should do something like
        # with path.open('r') as f:
        #   for object in some_library(f):
        #       yield (object.id, object.key)

    def _emit_history(self, upath: Path, cleaned) -> None:
        """
        calls extract_objects to extract lines from the unpacked path
        subclasses should override that to yield some kind of object
        out to here
        """
        with cleaned.open("w") as f:
            for line in self.extract_objects(upath):
                # newlines may interfere with the diffing, use the repr of the string
                f.write(repr(str(line)))
                f.write("\n")

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Normalised]:
        cleaned = unique_file_in_tempdir(input_filepath=path, dir=self.tmp_dir, suffix=path.suffix)

        self._emit_history(path, cleaned)
        sort_file(cleaned)

        yield cleaned


if __name__ == "__main__":
    ExtractObjectsNormaliser.main()
