from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from bleanser.core.modules.json import Json, JsonNormaliser, delkeys

TARGET = object()


def _check_and_extract(x, schema) -> Iterator[Any]:
    if schema is TARGET:
        yield x
        return
    if type(schema) == type:  # noqa: E721
        assert isinstance(x, schema), x
        return
    if type(schema) == list:  # noqa: E721
        [sch] = schema
        assert isinstance(x, list), x
        for i in x:
            yield from _check_and_extract(x=i, schema=sch)
        return

    assert type(schema) == dict, schema  # noqa: E721
    assert isinstance(x, dict), x

    xk = x.keys()
    sk = schema.keys()
    assert xk == sk, (sk, xk)
    for k in xk:
        yield from _check_and_extract(x=x[k], schema=schema[k])


def check_and_extract(x, schema) -> Any:
    [res] = list(_check_and_extract(x=x, schema=schema))
    return res


# TODO move to some generic helper
SCHEMA = {
    'meta': {
        'code': int,
        'requestId': str,
    },
    'notifications': [
        {
            'item': {
                'unreadCount': int,
            },
            'type': str,
        },
    ],
    'response': {
        'checkins': {
            'count': int,
            'items': TARGET,
        }
    },
}


class Normaliser(JsonNormaliser):
    PRUNE_DOMINATED = True
    # hmm, I guess makes sense to make MULTIWAY = False considering it seems to be cumulative... kinda safer this way
    # on the otherhand useful to keep multiway for renamed venues? ugh
    MULTIWAY = True

    def cleanup(self, j: Json) -> Json:
        # ok, a bit nasty -- foursquare export seems to be a list of some sort of responses..
        assert isinstance(j, list)

        res = []
        for d in j:
            l = check_and_extract(x=d, schema=SCHEMA)
            assert isinstance(l, list)
            res.extend(l)

        for c in res:
            # some id that might change, probs useless
            v = c.get('venue', None)
            if v is not None:
                v['contact'].pop('facebook', None)  # don't care
                v['contact'].pop('instagram', None)  # don't care
                v.pop('verified', None)  # don't care
                v.pop('delivery', None)  # eh, we don't care about what venue uses for delivery

            # todo would be nice to support compose keys for delkeys..
            # e.g. ('venue', 'contact', 'facebook')
            delkeys(
                c,
                keys={
                    ## these are just always changing, nothing we can do about it
                    'checkinsCount',
                    'usersCount',
                    'tipCount',
                    ##
                    'sticker',  # very volatile, some crap that 4sq sets on places
                    # ugh. lat/lng are volatile, varying after 4th digit after dot for some reason
                    'lat',
                    'lng',  # TODO instead round to 4th digit or something??
                },
            )

        return res


if __name__ == '__main__':
    Normaliser.main()
