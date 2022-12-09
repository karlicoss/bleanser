#!/usr/bin/env python3
from bleanser.modules.json_new import delkeys, patch_atoms
from bleanser.core.sqlite import SqliteNormaliser, Tool

import json


def _patch_volatile_urls(x):
    # these contain some sort of hashes and change all the time
    if not isinstance(x, str):
        return x
    if 'fbcdn.net' in x:
        return ""
    if 'cdninstagram' in x:
        return ""
    return x


def _cleanup_jsons(s):
    if s is None:
        return None

    if isinstance(s, bytes):
        j = json.loads(s.decode('utf8'))
    else:
        # hmm normally it's bytes, but on odd occasions (old databases??) was str? odd
        j = json.loads(s)

    delkeys(j, keys=[
        'interop_user_type',
        'is_group_xac_calling_eligible',
        'processed_business_suggestion',

        'url_expiration_timestamp_us',
        'is_eligible_for_igd_stacks',
        'profile_pic_url',  # volatile
        'all_media_count',
        'displayed_action_button_type',
        'is_epd',
        'liked_clips_count',
        'reel_media_seen_timestamp',
        'latest_besties_reel_media',
        'latest_fanclub_reel_media',
        'latest_reel_media',
    ])
    j = patch_atoms(j, patch=_patch_volatile_urls)
    return json.dumps(j, sort_keys=True).encode('utf8')


def _cleanup_jsons_2(s):
    try:
        return _cleanup_jsons(s)
    except Exception as e:
        raise e


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True


    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        msgs = tables['messages']
        assert 'timestamp' in msgs
        assert 'text' in msgs

        threads = tables['threads']


    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop('session')  # super volatile

        # so message/thread_info tables also contain a json field with raw data, and it's very volatile
        # to clean it up, tried using this at first:
        # SELECT _id, message_type, message, json_remove(message, (SELECT DISTINCT(fullkey) FROM messages, json_tree(message) WHERE atom LIKE '%cdninstagram%')) FROM messages ORDER BY message_type
        # it was promising, but it seems that it's not possible to pass multiple arguments from a scalar subquery
        # it only ended up removing the first key
        c.create_function("CLEANUP_JSONS", 1, _cleanup_jsons_2)
        queries = [
            'UPDATE messages SET message     = CLEANUP_JSONS(message)',
            'UPDATE threads  SET thread_info = CLEANUP_JSONS(thread_info)',
        ]
        for query in queries:
            list(c.execute(query))
        # a bit insane and experimental... but worked surprisingly smoothly and fast?
        ##


if __name__ == '__main__':
    Normaliser.main()
