#!/usr/bin/env python3
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


from bleanser.core.processor import BaseNormaliser
from bleanser.core.utils import Json


class Normaliser(BaseNormaliser):
    # TODO not sure if should override here? rely on parent class not having defaults
    # filter out additions; keep the rest
    DIFF_FILTER =  '> '

    DELETE_DOMINATED = False

    def cleanup(self, j: Json) -> Json:
        return j

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        with self.unpacked(path=path, wdir=wdir) as upath:
            pass
        del path # just to prevent from using by accident

        j = json.loads(upath.read_text())
        j = self.cleanup(j)

        # todo copy paste from SqliteNormaliser
        jpath = upath.absolute().resolve()
        cleaned = wdir / Path(*jpath.parts[1:]) / (jpath.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)

        with cleaned.open('w') as fo:
            if isinstance(j, list):
                j = {'<toplevel>': j} # meh

            assert isinstance(j, dict), j
            for k, v in j.items():
                if not isinstance(v, list):
                    # something like 'profile' data in hypothesis could be a dict
                    # something like 'notes' in rescuetime could be a scalar (str)
                    v = [v] # meh
                assert isinstance(v, list), (k, v)
                for i in v:
                    print(f'{k} ::: {json.dumps(i, sort_keys=True)}', file=fo)

        yield cleaned


if __name__ == '__main__':
    Normaliser.main()
