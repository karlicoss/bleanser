#!/usr/bin/env python3

def sqldiff(*args, **kwargs):
    from subprocess import check_call
    # todo iterator/yield stuff as we go (might allow for earlier termination)
    check_call(['sqldiff', *args], **kwargs)


### Bluemaestro
from .paths import BM1, BM2

def test_bluemaestro_old_vs_new():
    sqldiff(BM1, BM2)


def test_bluemaestro_new_vs_old():
    sqldiff(BM2, BM1)


def test_bluemaestro_old_vs_old():
    sqldiff(BM1, BM1)

'''
generally this seems to work well
TODO annoying bit is that bluemaestro has 'updates' like
  e.g. UPDATE ABCDEF_info SET downloadUnix=1614539578549 WHERE id=0;
'''
###


### podcastaddict
#
from .paths import PA1, PA2, PA3, PA4
def test_podcastaddict_2_vs_3():
    # PA2 and PA3 are a month apart
    # TODO shit -- this is hopeless, lots of completely random updates...
    # maybe triplets mode would be better...
    # TODO blog: motivation for 'triplets' pruning
    sqldiff(PA2, PA3)

def test_podcastaddict_3_vs_4():
    # these are a day apart -- ok, no changes..
    sqldiff(PA3, PA4)


'''
interesting tables:
- podcasts

there is a fair amount of crap tables, e.g.
- ad_campaign
- bitmaps
- radion_search_results?

but it would be annoying if this knowledge starts seeping through ....

'''

###


### firefox
'''
generally could work... but has some stuff like this:
UPDATE moz_meta SET value=368 WHERE "key"='origin_frecency_count';
UPDATE moz_meta SET value=47638 WHERE "key"='origin_frecency_sum';
UPDATE moz_meta SET value=79202478 WHERE "key"='origin_frecency_sum_of_squares';
UPDATE moz_origins SET frecency=6263 WHERE id=1;
'''
###



### general strategy?

'''
basically split diff operations in allowed/forbidden/unknown?

allowed:
  CREATE TABLE
  INSERT INTO (TODO not sure about on conflict??)

maybe allow some updates??


forbidden:
  DROP
  UPDATE
  DELETE

TODO maybe a safer option would be to dump a 'diff database'?? not sure how it would work...

'''


### TODO definitely need a 'simple' mode, for pruning exactly equal files..
### maybe run it first regardless, for performance reasons
