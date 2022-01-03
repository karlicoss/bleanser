#!/usr/bin/env python3
from bleanser.core.json import JsonNormaliser, delkeys, Json


class Normaliser(JsonNormaliser):
    # TODO hmm. do_cleanup should run in a parallel process.. otherwise it's basically not parallelizing here?
    def cleanup(self, j: Json) -> None:
        ## these change for no reason, and probably no one cares about them
        delkeys(j, keys={
            'images',
            'available_markets',
            'popularity',
            'preview_url',
            'external_urls',
            'total_episodes',
        })
        ##

        # TODO hmm. it changes often... but then it's kind of a useful info..
        # del j['recently_played']


if __name__ == '__main__':
    Normaliser.main()
