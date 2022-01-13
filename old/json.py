#!/usr/bin/env python3
from __future__ import annotations

from contextlib import contextmanager
from itertools import tee
import orjson as json
from pathlib import Path
from typing import Iterator, List

from bleanser.core.common import logger
from bleanser.core.utils import Json
from bleanser.core.processor import BaseNormaliser


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

import hashlib

from typing import Iterator, Tuple, Iterable
JPath = str
JVal  = str
JHash = str
# TODO ugh. it's a bit too elaborate way to do structural diff, really...
# TODO fuck. this is quite slow, but not sure what should I do about it...
# how to make it work with process pool executor??
def _aspaths(js: Json) -> Tuple[JHash, Iterable[Tuple[JPath, JVal]]]:
    if isinstance(js, (str, int, float, bool, type(None))):
        # TODO json dumps?
        # TODO do root values really need hash?
        vhash = hashlib.md5(str(js).encode('utf8')).hexdigest()[:7]
        return (vhash, [('', str(js))])

    sep = '.' # todo customize?

    # TODO ugh. not very iterative..
    # I guess can't really be, because need information about all siblings before proceeding?
    if isinstance(js, list):
        ress = []
        for i, c in enumerate(js):
            k = str(i)
            chash, cres = _aspaths(c)

            for p, v in cres:
                cp = chash
                ress.append((cp + ('' if len(p) == 0 else (sep + p)), v))
        # TODO list shouldn't be hashed??
        # TODO shit... could this be a problem for something like tags?
        return ('<list>', ress)

    if isinstance(js, dict):
        # TODO or maybe two pass? then won't need to cache as much?
        # TODO could optimize and avoid combining the very top level hash?
        ress = []
        hd: dict[str, str] = {}
        for k, c in sorted(js.items()):
            cp = k

            chash, cres = _aspaths(c)
            hd[k] = chash

            for p, v in cres:
                ress.append((cp + ('' if len(p) == 0 else (sep + p)), v))

        dhash = hashlib.md5(json.dumps(hd)).hexdigest()[:7]
        return (dhash, ress)

    raise RuntimeError(js, type(js))


def aspaths(js: Json) -> Iterator[str]:
    _, res = _aspaths(js=js)
    for k, v in res:
        yield k + ' : ' + v


def test_aspaths() -> None:
    j = {
        'root': [
            dict(a=1,b=1),
            dict(a=1,b=0),
            dict(a=0,b=1),
            dict(a=0,b=0),
            dict(a=2,b=2),

            dict(a=1,b=0),
            dict(a=1,b=1),
        ],
        'boop': {'beep': [123, 456]},
    }
    paths = list(aspaths(j))
    assert paths == [
        'boop.beep.202cb96 : 123',
        'boop.beep.250cf8b : 456',
        'root.824ad40.a : 1',
        'root.824ad40.b : 1',
        'root.8a5a377.a : 1',
        'root.8a5a377.b : 0',
        'root.23bbe1a.a : 0',
        'root.23bbe1a.b : 1',
        'root.213c309.a : 0',
        'root.213c309.b : 0',
        'root.8b165c4.a : 2',
        'root.8b165c4.b : 2',
        'root.8a5a377.a : 1',
        'root.8a5a377.b : 0',
        'root.824ad40.a : 1',
        'root.824ad40.b : 1',
    ]



def _aspaths_aux(js: Json) -> List[str]:
    return list(aspaths(js))


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
        # TODO call .unpacked

        # todo copy paste from SqliteNormaliser
        path = path.absolute().resolve()
        cleaned = wdir / Path(*path.parts[1:]) / (path.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)

        with path.open('r') as fp:
            j = json.loads(fp.read())
        self.cleanup(j)
        # todo sort keys? not sure...
        # TODO huh. jq version is almost order of magnitude slower???
        # js = json.dumps(j) # , indent=2, sort_keys=True)
        # cmd = jq['-r', JQ_PATHS]
        # jq_lines = (cmd << js )().splitlines()
        jq_lines = _aspaths_aux(j)
        # # move to top
        # from concurrent.futures import ProcessPoolExecutor as Pool
        # pool = Pool(8)
        # #
        # fut = pool.submit(_aspaths_aux, j)
        # jq_lines = fut.result()

        # TODO later
        cleanup_jq_dump = getattr(self, 'cleanup_jq_dump', None)
        if cleanup_jq_dump is not None:
            cleanup_jq_dump(jq_lines)
        with cleaned.open('w') as fp:
            for line in jq_lines:
                print(line, file=fp)
        yield cleaned



def test_json_normaliser_1(tmp_path: Path) -> None:
    j = [
        dict(a=1,b=1),
        dict(a=1,b=0),
        dict(a=0,b=1),
        dict(a=0,b=0),
        dict(a=2,b=2),

        dict(a=1,b=0),
        dict(a=1,b=1),
    ]
    i = tmp_path / 'input.json'
    i.write_text(json.dumps(j))

    n = JsonNormaliser()
    with n.do_cleanup(i, wdir=tmp_path) as c:
        res = c.read_text()

    lines = res.splitlines()
    assert len(lines) == 14, lines

    lset = set(lines)
    # we want to keep these unique 'rows'
    assert len(lset) == 10, (lines, lset)


def test_json_normaliser_2(tmp_path: Path) -> None:
    # TODO ok -- so we need to mark certain 'levels' as rolling instead? uggggh
    j = [
        ['b', 1],
        ['b', 0],
        ['a', 1],
        ['a', 0],
        ['c', 2],

        ['b', 0],
        ['b', 1],
    ]
    i = tmp_path / 'input.json'
    i.write_text(json.dumps(j))

    n = JsonNormaliser()
    with n.do_cleanup(i, wdir=tmp_path) as c:
        res = c.read_text()

    lines = res.splitlines()
    assert len(lines) == 14, lines

    lset = set(lines)
    # TODO right, this won't work now... because we don't want to hash the whole list...
    # assert len(lset) == 10, (lines, lset)


# can work as generic json processor
if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=JsonNormaliser)

# just for convenience
from .utils import Json
