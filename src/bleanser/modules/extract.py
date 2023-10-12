from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Any


from bleanser.core.processor import BaseNormaliser, unique_file_in_tempdir, sort_file


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
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        with self.unpacked(path, wdir=wdir) as upath:
            cleaned = unique_file_in_tempdir(input_filepath=path, wdir=wdir, suffix=path.suffix)
            del path

            self._emit_history(upath, cleaned)
            sort_file(cleaned)

        yield cleaned


if __name__ == "__main__":
    ExtractObjectsNormaliser.main()
