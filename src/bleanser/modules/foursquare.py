#!/usr/bin/env python3
from __future__ import annotations

from bleanser.core.json import delkeys
from bleanser.modules.json_new import Normaliser as JsonNormaliser, Json


TARGET = object()

from typing import Iterator, Optional, Any
def _check_and_extract(x, schema) -> Iterator[Any]:
    if schema is TARGET:
        yield x
        return
    if type(schema) == type:
        assert isinstance(x, schema), x
        return
    if type(schema) == list:
        [sch] = schema
        assert isinstance(x, list), x
        for i in x:
            yield from _check_and_extract(x=i, schema=sch)
        return

    assert type(schema) == dict, schema
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
    }
}

class Normaliser(JsonNormaliser):
    DELETE_DOMINATED = True
    # hmm, I guess makes sense to make MULTIWAY = False considering it seems to be cumulative... kinda safer this way
    # on the otherhand useful to keep multiway for renamed venues? ugh
    MULTIWAY = True

    def cleanup(self, j: Json) -> Json:
        # ok, a bit nasty -- foursquare export seems to be a list of some sort of responces..
        assert isinstance(j, list)

        res = []
        for d in j:
            l = check_and_extract(x=d, schema=SCHEMA)
            assert isinstance(l, list)
            res.extend(l)

        for c in res:
            delkeys(c, keys={
                ## these are just always changing, nothing we can do about it
                'checkinsCount',
                'usersCount',
                'tipCount',
                ##

                'sticker', # very volatile, some crap that 4sq sets on places

                # ugh. lat/lng are volatile, varying after 4th digit after dot for some reason
                'lat', 'lng', # FIXME instead round to 4th digit or something??
            })

        return res

    # old processor for json -- probably can remove now
    # def cleanup_jq_dump(self, lines: list[str]) -> None:
    #     import re
    #     # FIXME X is a bit non-standard -- think about it better
    #     to_drop = [
    #         # too volatile
    #         r'^X.(meta|notifications).',
    #         r'.photo.(prefix|suffix)',
    #         r'.photos.items.X.(prefix|suffix)',
    #         r'^X.response.checkins.count',
    #         r'.X.editableUntil',

    #         # r'.items.X.(venue|sticker).',
    #         r'.items.X.sticker.',
    #         r'.items.X.venue.stats.(usersCount|tipCount|checkinsCount)',
    #         r'.items.X.venue.(menu|url|categories|contact|delivery|location.labeledLatLng|location.lat|location.lng)',
    #         r'.items.X.venue.(location.lat|location.lng)',
    #     ]
    #     res = []
    #     for line in lines:
    #         drop = any(re.search(r, line) for r in to_drop)
    #         if not drop:
    #             res.append(line)
    #     lines[:] = res


if __name__ == '__main__':
    Normaliser.main()
