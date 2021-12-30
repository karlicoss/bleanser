#!/usr/bin/env python3
from pathlib import Path
import sys
from subprocess import check_call

import json

# todo hmm, seems that ther isn't that much perf difference, at least on hyperfine
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


def preprocess(*, j, name):
    # todo not sure how defensive should be?

    # todo not sure if there is a better way
    if '/github-events/' in name:
        pp_github(j)


def process(fo, *, name) -> None:
    j = json.loads(fo.read())
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


def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('path1')
    p.add_argument('path2', nargs='?')
    args = p.parse_args()

    p1 = args.path1
    p2 = args.path2

    # TODO compare performance fo handling compressed and uncompressed files
    from bleanser.core.kompress import CPath

    if p2 is None:
        # handle single file
        if p1 == '-':
            process(fo=sys.stdin)
        else:
            path = str(Path(p1).absolute())
            with CPath(path).open() as fo:
                process(fo=fo, name=path)
    else:
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
        # FIXME pipefail? doesn't work well..
        cmd = f'vimdiff {wrap} <({c1}) <({c2})'
        check_call(cmd, shell=True, executable='/bin/bash')


if __name__ == '__main__':
    main()
