#!/usr/bin/env python3
from bleanser.core.utils import Json
from bleanser.core.json import JsonNormaliser, delkey


class SpotifyNormaliser(JsonNormaliser):
    # TODO hmm. do_cleanup should run in a parallel process.. otherwise it's basically not parallelizing here?
    def cleanup(self, j: Json) -> None:
        ## these change for no reason, and probably no one cares about them
        delkey(j, key='images')
        delkey(j, key='available_markets')
        delkey(j, key='popularity')
        delkey(j, key='preview_url')
        delkey(j, key='external_urls')
        delkey(j, key='total_episodes')
        ##

        # TODO hmm. it changes often... but then it's kind of a useful info..
        # del j['recently_played']


if __name__ == '__main__':
    from bleanser.core import main
    main(Normaliser=SpotifyNormaliser)
