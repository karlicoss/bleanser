#!/usr/bin/env python3
from bleanser.core.modules.json import JsonNormaliser, Json, delkeys


class Normaliser(JsonNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        del j['since']  # flaky
        return j


if __name__ == '__main__':
    Normaliser.main()
