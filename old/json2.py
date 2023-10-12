#!/usr/bin/env python3
from pathlib import Path
import sys
from subprocess import check_call

import json

# todo hmm, seems that there isn't that much perf difference, at least on hyperfine
# although on the profile, when running with orjson, seems to finish faster??
# maybe double check later
# import orjson as json


# TODO warn about some data being cleaned, refer to the sources
def pp_github(j):
    # todo later compare to jq somehow? but doubt it'd be faster
    from itertools import chain

    # TODO hmm
    # what should we do with repos :::: clones thing?
    # maybe we could check domination relationship in a more clever way somehow?...
    # e.g. here clones -> count ???
    # ': 0, 'uniques': 0, 'views': []}, 'clones': {'count': 29, 'uniques': 14, 'clones': [{'timestamp': '2021-11-29T00:00:00Z'|       'pull': True}, 'traffic': {'views': {'count': 0, 'uniques': 0, 'views': []}, 'clones': {'count': 27, 'uniques': 13, 'cl
    #   , 'count': 3, 'uniques': 2}, {'timestamp': '2021-11-30T00:00:00Z', 'count': 2, 'uniques': 1}, {'timestamp': '2021-12-01T|       ones': [{'timestamp': '2021-11-29T00:00:00Z', 'count': 1, 'uniques': 1}, {'timestamp': '2021-11-30T00:00:00Z', 'count':

    # TODO not sure what to do with it...
    # for x in j['repos']:
    #     del x['traffic']

    for x in chain(j['watched'], j['starred']):
        for key in [
                'watchers', 'stargazers_count', 'watchers_count',

                # updated_at -- seems that it's updated every time there is a star etc...
                'updated_at',
                'forks', 'forks_count',

                'open_issues', 'open_issues_count',

                # eh, not sure about these...
                'pushed_at',
                'size',
        ]:
            del x[key]


def pp_spotify(j):
    from bleanser.modules.spotifyexport import Normaliser
    n = Normaliser(path='meh')
    # todo method to delete multiple keys
    n.cleanup(j=j)


    # TODO need to unflatten playlists somehow
    # hmm basically any list-like thing is 'suspicious', because it kinda means denormalised struct
    pl2 = []
    for x in j['playlists']:
        for t in x['tracks']:
            q = {k: v for k, v in x.items()}
            q['tracks'] = t
            pl2.append(q)
    j['playlists'] = pl2
    # hmm this is annoying... shared playlists are updating literally every day?


def preprocess(*, j, name):
    # todo not sure how defensive should be?

    # todo not sure if there is a better way
    if '/github-events/' in name:
        pp_github(j)
    elif '/spotify/' in name:
        pp_spotify(j)


def process(fo, *, name) -> None:
    data = fo.read()
    # todo orjson supports memoryview??
    j = json.loads(data)
    # todo would be nice to close it here

    preprocess(j=j, name=name)

    if isinstance(j, list):
        res = {'<toplevel>': j} # meh
    else:
        assert isinstance(j, dict), j
        res = j

    for k, v in res.items():
        if not isinstance(v, list):
            # something like 'profile' data in hypothesis could be a dict
            # something like 'notes' in rescuetime could be a scalar (str)
            v = [v] # meh
        assert isinstance(v, list), (k, v)
        for i in v:
            # todo dump json here for i; sort keys?
            print(f'{k} ::: {i}')
    print('ok')


def compare(p1: str, p2: str):
    assert p1 != '-' and p2 != '-'
    # hacky way to compare
    def cc(p: str):
        if p.endswith('.xz'):
            cat = 'xzcat'
        else:
            cat = 'cat'
        # {cat} {p} | {__file__} -
        return f'{__file__} {p} | sort'
    c1 = cc(p1)
    c2 = cc(p2)
    # wrap = ' -c "windo set wrap" '#  -- eh, not super convenient?
    wrap = ''
    # TODO pipefail? doesn't work well..
    cmd = f'vimdiff {wrap} <({c1}) <({c2})'
    check_call(cmd, shell=True, executable='/bin/bash')


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('path1')
    p.add_argument('path2', nargs='?')
    p.add_argument('--first' , required=False, type=int)
    p.add_argument('--second', required=False, type=int)
    args = p.parse_args()

    p1 = args.path1
    p2 = args.path2

    # TODO compare performance fo handling compressed and uncompressed files
    from bleanser.core.kompress import CPath

    assert p1 is not None

    if p2 is not None:
        compare(p1=p1, p2=p2)
        return

    # handle single file
    if p1 == '-':
        process(fo=sys.stdin)
        return

    pp = Path(p1).absolute()

    if pp.is_dir():
        files = list(sorted(pp.iterdir()))

        first = args.first; assert first is not None

        second = args.second
        if second is None:
            second = first + 1
        assert second < len(files), len(files)

        p1 = str(files[first ])
        p2 = str(files[second])
        compare(p1=p1, p2=p2)
    else:
        path = str(pp)
        with CPath(path).open() as fo:
            process(fo=fo, name=path)


if __name__ == '__main__':
    main()
