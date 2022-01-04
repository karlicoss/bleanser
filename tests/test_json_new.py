from common import skip_if_not_karlicoss as pytestmark

from pathlib import Path
from typing import List

import pytest

from bleanser.modules.json_new import Normaliser

from common import TESTDATA, actions, hack_attribute


# TODO actually implement some artificial json test


# TODO pinboard: tag summaries might be flaky
# might be worth doing
# if isinstance(j, dict):
#     del j['tags']

def test_pinboard() -> None:
    data = TESTDATA / 'pinboard'

    paths = list(sorted(data.glob('*.json')))

    with hack_attribute(Normaliser, 'MULTIWAY', True), hack_attribute(Normaliser, 'DELETE_DOMINATED', True):
        res = actions(paths=paths, Normaliser=Normaliser)

    # note: some items duplicate in pinboard...
    # e.g. in bookmarks_2019-08-06.json.xz
    # <toplevel> ::: {"description": "Visual Leak Detector - Enhanced Memory Leak Detection for Visual C++ - CodeProject", "extended": "", "hash": "ef6dcf9d2987ea1f4919b31024c33662", "href": "http://www.codeproject.com/KB/applications/visualleakdetector.aspx", "meta": "8341db79448607b145078e00e69c8003", "shared": "yes", "tags": "debugging cpp", "time": "2014-02-09T01:02:57Z", "toread": "no"}

    assert [p.name for p in res.remaining] == [

        'bookmarks_2019-08-06.json'      , # first in group
        # fully contained in the next
        # 'bookmarks_2019-08-07.json'      , : MOVE

        # has to keep thie next because for example this bookmark is flaky:
        # rg 'An Easy Explaination Of First And Follow Sets' | sort
        # bookmarks_2019-08-07.json:{"href":"http:\/\/www.jambe.co.nz\/UNI\/FirstAndFollowSets.html","description":"An Easy Explaination Of First And Follow Sets","extended":"","meta":"c68c6b649d587543bae12367e6fce8ec","hash":"3688a0bcfb0ee9f7cb7fbda43aabe131","time":"2014-02-09T01:03:03Z","shared":"yes","toread":"no","tags":"cs parsing"},
        # bookmarks_20190924T010105Z.json:{"href":"http:\/\/www.jambe.co.nz\/UNI\/FirstAndFollowSets.html","description":"An Easy Explaination Of First And Follow Sets","extended":"","meta":"c68c6b649d587543bae12367e6fce8ec","hash":"3688a0bcfb0ee9f7cb7fbda43aabe131","time":"2014-02-09T01:03:03Z","shared":"yes","toread":"no","tags":"cs parsing"},
        # bookmarks_20190929T124250Z.json:   "description": "An Easy Explaination Of First And Follow Sets",
        # pinboard_20201231T011022Z.json:   "description": "An Easy Explaination Of First And Follow Sets",
        # pinboard_20210220T011105Z.json:   "description": "An Easy Explaination Of First And Follow Sets",
        # pinboard_20210221T011013Z.json:   "description": "An Easy Explaination Of First And Follow Sets",
        # pinboard_20220103T011019Z.json:   "description": "An Easy Explaination Of First And Follow Sets",
        'bookmarks_20190924T010105Z.json', #: will keep

        # there is a whole bunch of flaky bookmarks like that ^ in pinboard, so won't bother annotating the rest

        # 'bookmarks_20190925T010106Z.json', : MOVE
        'bookmarks_20190929T010107Z.json', #: will keep
        'bookmarks_20190929T124250Z.json', #: will keep
        # 'bookmarks_20190930T010107Z.json', : MOVE
        # 'bookmarks_20191015T010107Z.json', : MOVE
        # 'bookmarks_20191016T010107Z.json', : MOVE
        # 'bookmarks_20191122T010108Z.json', : MOVE
        # 'bookmarks_20191123T010107Z.json', : MOVE
        'bookmarks_20191205T010108Z.json', #: will keep
        # 'bookmarks_20191206T010107Z.json', : MOVE
        # 'bookmarks_20191207T010107Z.json', : MOVE
        'pinboard_20200501T011005Z.json' , #: will keep
        # 'pinboard_20200502T011005Z.json' , : MOVE
        'pinboard_20200614T011006Z.json' , #: will keep
        'pinboard_20200615T001107Z.json' , #: will keep
        # 'pinboard_20200616T001008Z.json' , : MOVE
        # 'pinboard_20200812T001014Z.json' , : MOVE
        # 'pinboard_20200813T001016Z.json' , : MOVE
        # 'pinboard_20200814T001018Z.json' , : MOVE
        # 'pinboard_20200815T001017Z.json' , : MOVE
        'pinboard_20200826T001017Z.json' , #: will keep
        # 'pinboard_20200827T001019Z.json' , : MOVE
        'pinboard_20201230T011025Z.json' , #: will keep
        # 'pinboard_20201231T011022Z.json' , : MOVE
        # 'pinboard_20210220T011105Z.json' , : MOVE
        # 'pinboard_20210221T011013Z.json' , : MOVE
        'pinboard_20220103T011019Z.json' , #: will keep
    ]


def test_nonidempotence(tmp_path: Path) -> None:
    '''
    Just demonstrates that multiway processing might be
    It's probably going to be very hard to fix, likely finding 'minimal' cover (at least in terms of partial ordering) is NP hard?
    '''

    sets = [
        [],
        ['a'],
        ['a', 'b'],
        [     'b', 'c'],
        ['a', 'b', 'c'],
    ]
    import json
    for i, s in enumerate(sets):
        p = tmp_path / f'{i}.json'
        p.write_text(json.dumps(s))

    with hack_attribute(Normaliser, 'MULTIWAY', True), hack_attribute(Normaliser, 'DELETE_DOMINATED', True):
        paths = list(sorted(tmp_path.glob('*.json')))
        res = actions(paths=paths, Normaliser=Normaliser)

        assert [p.name for p in res.remaining] == [
            '0.json', # keeping as boundary
            '2.json', # keeping because item a has rolled over
            '4.json', # keeping as boundary
        ]

        paths = list(res.remaining)
        res = actions(paths=paths, Normaliser=Normaliser)
        assert [p.name for p in res.remaining] == [
            '0.json',
            # note: 2.json is removed because fully contained in 4.json
            '4.json',
        ]
