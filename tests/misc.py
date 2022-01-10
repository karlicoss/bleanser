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
### TODO not sure how to make it friednly to DAL...
### eh. whatever, these aren't the most pressing issues... people have lots of disk space and very few sync it
### maybe just add it on the roadmap
