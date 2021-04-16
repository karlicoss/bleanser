#!/usr/bin/env python3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from bleanser.core.common import logger
from bleanser.core.utils import Json
from bleanser.core.sqlite import BaseNormaliser


from plumbum import local  # type: ignore


jq = local['jq']


# TODO hmm maybe I just want to use https://github.com/tomnomnom/gron ?
# although would be tricky to chop off the indices...

# we replace numbers with placeholders since otherwise it's too unstable
# TODO ... not sure if it should be the default
JQ_PATHS = '''
paths(scalars) as $p
  | [ ( [ $p[] | if type == "number" then "X" else tostring end ] | join(".") )
    , ( getpath($p) | tojson )
    ]
  | join(": ")
'''

class JsonNormaliser(BaseNormaliser):
    # filter out additions; keep the rest
    DIFF_FILTER = '> '

    MULTIWAY = True
    # TODO delete dominated

    def cleanup(self, j: Json) -> None:
        # TODO not sure if should modify in place?
        pass

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        # todo copy paste from SqliteNormaliser
        path = path.absolute()
        cleaned = wdir / Path(*path.parts[1:]) / (path.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)

        import json
        with path.open('r') as fp:
            j = json.load(fp)
        self.cleanup(j)
        # todo sort keys? not sure...
        js = json.dumps(j) # , indent=2, sort_keys=True)
        cmd = jq['-r', JQ_PATHS]
        jq_lines = (cmd << js )().splitlines()

        # TODO later
        cleanup_jq_dump = getattr(self, 'cleanup_jq_dump', None)
        if cleanup_jq_dump is not None:
            cleanup_jq_dump(jq_lines)
        with cleaned.open('w') as fp:
            for line in jq_lines:
                print(line, file=fp)
        yield cleaned


def delkey(j: Json, *, key: str) -> None:
    # todo if primitive, don't do anything
    if   isinstance(j, (int, float, bool, type(None), str)):
        return
    elif isinstance(j, list):
        for v in j:
            delkey(v, key=key)
    elif isinstance(j, dict):
        j.pop(key, None)
        for k, v in j.items():
            delkey(v, key=key)
    else:
        raise RuntimeError(type(j))


# can work as generic json processor
if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=JsonNormaliser)

# just for convenience
from .utils import Json
