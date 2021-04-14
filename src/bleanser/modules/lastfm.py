#!/usr/bin/env python3
from bleanser.core.json import JsonNormaliser, delkey, Json


class Normaliser(JsonNormaliser):
    DELETE_DOMINATED = True
    def cleanup(self, j: Json) -> None:
        # ugh sometimes case changes for no reason
        # TODO would be nice to use jq for that... e.g. older filter was
        # 'sort_by(.date) | map(map_values(ascii_downcase?))'
        for x in j:
        # TODO json can be list...
            for k, v in x.items():  # type: ignore
                x[k] = v.lower()


if __name__ == '__main__':
    Normaliser.main()
