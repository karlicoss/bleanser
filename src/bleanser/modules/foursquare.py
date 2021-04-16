#!/usr/bin/env python3
from __future__ import annotations

from bleanser.core.json import JsonNormaliser, delkey, Json


class Normaliser(JsonNormaliser):
    DELETE_DOMINATED = True
    # hmm, I guess makes sense to make MULTIWAY = False considering it seems to be cumulative... kinda safer this way
    # on the otherhand useful to keep multiway for renamed venues? ugh
    MULTIWAY = True

    # TODO rename to cleanup_json?
    def cleanup(self, j: Json) -> None:
        pass

    # TODO this is actually kinda similar to what sql dump is doing
    # so first we're procesisng it as a native structure
    # and then finally with grep?
    # hmmm
    def cleanup_jq_dump(self, lines: list[str]) -> None:
        import re
        # FIXME X is a bit non-standard -- think about it better
        to_drop = [
            # too volatile
            r'^X.(meta|notifications).',
            r'.photo.(prefix|suffix)',
            r'.photos.items.X.(prefix|suffix)',
            r'^X.response.checkins.count',
            r'.X.editableUntil',

            # r'.items.X.(venue|sticker).',
            r'.items.X.sticker.',
            r'.items.X.venue.stats.(usersCount|tipCount|checkinsCount)',
            r'.items.X.venue.(menu|url|categories|contact|delivery|location.labeledLatLng|location.lat|location.lng)',
            # lat/lng are volatile, vary after 4th digit after dot for some reason
            r'.items.X.venue.(location.lat|location.lng)',
        ]
        res = []
        for line in lines:
            drop = any(re.search(r, line) for r in to_drop)
            if not drop:
                res.append(line)
        lines[:] = res

if __name__ == '__main__':
    Normaliser.main()
