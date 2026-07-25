from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from my.rtm import DAL, MyTodo

from bleanser.core.modules.extract import ExtractObjectsNormaliser


class Normaliser(ExtractObjectsNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def extract_objects(self, path: Path) -> Iterator[MyTodo]:
        # HPI's DAL extracts VTODO objects and deliberately excludes calendar-level metadata.
        # Preserve physical line endings because universal-newline conversion breaks RTM's malformed folded lines.
        with path.open(newline='') as stream:
            yield from DAL(data=stream.read()).all_todos()


if __name__ == '__main__':
    Normaliser.main()
