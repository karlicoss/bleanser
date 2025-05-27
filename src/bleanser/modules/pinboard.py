from bleanser.core.modules.json import JsonNormaliser


class Normaliser(JsonNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True


if __name__ == '__main__':
    Normaliser.main()


# TODO pinboard: tag summaries might be flaky
# might be worth doing
# if isinstance(j, dict):
#     del j['tags']


def test_pinboard() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions

    data = TESTDATA / 'pinboard'

    paths = sorted(data.glob('*.json'))

    res = actions(paths=paths, Normaliser=Normaliser)

    # note: some items duplicate in pinboard...
    # e.g. in bookmarks_2019-08-06.json.xz
    # <toplevel> ::: {"description": "Visual Leak Detector - Enhanced Memory Leak Detection for Visual C++ - CodeProject", "extended": "", "hash": "ef6dcf9d2987ea1f4919b31024c33662", "href": "http://www.codeproject.com/KB/applications/visualleakdetector.aspx", "meta": "8341db79448607b145078e00e69c8003", "shared": "yes", "tags": "debugging cpp", "time": "2014-02-09T01:02:57Z", "toread": "no"}

    assert [p.name for p in res.remaining] == [

        'bookmarks_2019-08-06.json'      , # first in group
        # fully contained in the next
        # 'bookmarks_2019-08-07.json'      , : MOVE

        # has to keep the next because for example this bookmark is flaky:
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
    ]  # fmt: skip
