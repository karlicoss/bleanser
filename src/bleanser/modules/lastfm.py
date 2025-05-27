from bleanser.core.modules.json import Json, JsonNormaliser


class Normaliser(JsonNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        # ugh sometimes case changes for no reason
        for x in j:
            for k, v in list(x.items()):
                if isinstance(v, str):
                    # defensive, there was a date (around 2019-01-16) when dates glitched and were ints...
                    x[k] = v.lower()
        return j
        # todo would be nice to use jq for that... e.g. older filter was
        # 'sort_by(.date) | map(map_values(ascii_downcase?))'


if __name__ == '__main__':
    Normaliser.main()


def test_lastfm() -> None:
    """
    This test also highlights how multiway cleanup is more efficient than twoway
    """
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions, hack_attribute

    data = TESTDATA / 'lastfm'
    paths = sorted(data.glob('*.json'))

    with hack_attribute(Normaliser, key='MULTIWAY', value=False):
        res = actions(paths=paths, Normaliser=Normaliser)
    assert [p.name for p in res.pruned] == [
        'lastfm_20211107T011431Z.json',  # fully contained in lastfm_20211127T011459Z
    ]

    with hack_attribute(Normaliser, key='MULTIWAY', value=True):
        res = actions(paths=paths, Normaliser=Normaliser)
    assert [p.name for p in res.remaining] == [
        'lastfm_2017-08-29.json',   # keeping : initial: X + a

        # disappeared (a), and a bunch of items added (Y)
        # (a) <toplevel> ::: {"album": "", "artist": "pusha t/haim/q-tip/stromae/lorde", "date": "1503868125", "name": "meltdown (\u0438\u0437 \u0444\u0438\u043b\u044c\u043c\u0430 \u00ab\u0433\u043e\u043b\u043e\u0434\u043d\u044b\u0435 \u0438\u0433\u0440\u044b: \u0441\u043e\u0439\u043a\u0430-\u043f\u0435\u0440\u0435\u0441\u043c\u0435\u0448\u043d\u0438\u0446\u0430\u00bb. \u0447\u0430\u0441\u0442\u044c i)"}
        # 'lastfm_2017-09-01.json', # removing:          X     + Y

        # bunch of items were added (Z + b)
        'lastfm_2017-09-19.json',   # keeping :          X     + Y + Z + b

        # but b disappeared in this: so the previous item is the last pivot
        # (b) <toplevel> ::: {"album": "", "artist": "denny berthiaume", "date": "1505649846", "name": "moon river"}
        # 'lastfm_2017-09-22.json', # removing:          X     + Y + Z     + Q

        'lastfm_2017-10-31.json',   # keeping : last item in group

        # this item is only present in this file:
        # <toplevel> ::: {"album": "departed glories", "artist": "biosphere", "date": "1635619124", "name": "than is the mater"}
        'lastfm_20211031T001458Z.json',

        # this item is only present in this file:
        # > <toplevel> ::: {"album": "2010", "artist": "earl sweatshirt", "date": "1638578097", "name": "2010"}
        'lastfm_20211204T011641Z.json',

        # last item
        'lastfm_20220103T011522Z.json',
    ]  # fmt: skip
