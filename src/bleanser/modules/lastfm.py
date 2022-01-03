#!/usr/bin/env python3
from bleanser.modules.json_new import Normaliser as JsonNormaliser, Json


class Normaliser(JsonNormaliser):

    MULTIWAY = True
    DELETE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        # ugh sometimes case changes for no reason
        for x in j:
            for k, v in x.items():
                x[k] = v.lower()
        return j
        # todo would be nice to use jq for that... e.g. older filter was
        # 'sort_by(.date) | map(map_values(ascii_downcase?))'


if __name__ == '__main__':
    Normaliser.main()
