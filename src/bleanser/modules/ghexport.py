#!/usr/bin/env python3
from bleanser.core.json import JsonNormaliser, delkey, Json


class Normaliser(JsonNormaliser):
    def cleanup(self, j: Json) -> None:
        # TODO hmm... need a way to cleanup after it's been dumped to json with paths... ugh
        # TODO ugh... for these various stats are always flaky (like number of starts) and I don't necessarily wanna track them...
        del j['watched']
        del j['starred']
        # TODO crap. traffic changing all the time.. and I want to keep it


if __name__ == '__main__':
    Normaliser.main()
