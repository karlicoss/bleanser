#!/usr/bin/env python3
from subprocess import check_call

def sqldiff(*args, **kwargs):
    # todo iterator/yield stuff as we go (might allow for earlier termination)
    check_call(['sqldiff', *args], **kwargs)


# 'triples' comparison...
# vimdiff <( (sqlite3 "file://$FILE1?immutable=1" '.dump' && sqlite3 "file://$FILE3?immutable=1" '.dump') | sort)  <(sqlite3 "file://$FILE2?immutable=1" '.dump' | sort)
def sqldump(path, **kwargs):
    # sqlite3 'file:podcastAddict.db?immutable=1' '.dump'
    check_call(['sqlite3', f'file:///{path}?immutable=1', '.dump'], **kwargs)


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


def test_podcastaddict_234():
    sqldump(PA2)
    # ugh crap. triples isn't much better...
    # e.g. stuff in 'podcasts' may reorder...

'''
interesting tables:
- podcasts

there is a fair amount of crap tables, e.g.
- ad_campaign
- bitmaps
- radion_search_results?
- content_policy_violation
- ordered_list???
- sqlite_stat1??
- fts_virtual_episode_docsize??
- blocking_services???

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


similar with 'triples' -- inevitably some moz_places increment...
shit. so does it really have to be schema aware?
maybe it's possible to have as little schema interventions as possible?

also
--strict mode -- absolutely conforms to schema
--???    mode -- just looks at 'useful' data (similarly to two 'styles' I have in jq normaliser)

'''
###


### smscalls
'''
xml file... hmm.
date changes & also total count & backup_set field
I guess the most reasonable is to have some xpath substitution...
overall probably not worth too much effort to generalize considering it's basically the only xml I have
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
### TODO needs great support for archives... also make sure to compare archives without unpacking at first
### TODO archives: optimize for decompression speed?
### TODO not sure how to make it friednly to DAL...
### eh. whatever, these aren't the most pressing issues... people have lots of disk space and very few sync it
### maybe just add it on the roadmap
