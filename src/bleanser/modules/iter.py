from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Any


from bleanser.core.processor import BaseNormaliser


class IterNormaliser(BaseNormaliser):
    """
    This is meant to be overrided by a subclass

    parse_file recieves an input file, and should yield data/objects that when converted
    to a string, produces some comparable data/object to the cleaned output file

    possible things this could return is a unique key/id, or a tuple of (key, data), or a
    namedtuple/dataclass

    newlines are stripped from the string, so lines can be compared/diffed properly

    Its possible you could use a library or code from https://github.com/karlicoss/HPI
    in parse_file, to use the DAL itself to parse the file https://beepb00p.xyz/exports.html#dal
    """

    def parse_file(self, path: Path) -> Iterator[Any]:
        raise NotImplementedError
        # when you subclass, you should do something like
        # with path.open('r') as f:
        #   for object in some_library(f):
        #       yield (object.id, object.key)

    def _emit_history(self, upath: Path, cleaned) -> None:
        """
        calls parse_file to extract lines from te unpacked path
        subclasses should override that to yield some kind of object
        out to here
        """
        with cleaned.open("w") as f:
            for line in self.parse_file(upath):
                # newlines may interfere with the diffing, as we sort the lines
                no_newlines = str(line).replace("\n", "").replace("\r", "")
                f.write(no_newlines)
                f.write("\n")

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        with self.unpacked(path, wdir=wdir) as upath:
            cleaned = self.unique_file_in_tempdir(path, wdir=wdir, suffix=path.suffix)
            del path

            self._emit_history(upath, cleaned)
            self.sort_file(cleaned)

        yield cleaned


if __name__ == "__main__":
    IterNormaliser.main()
