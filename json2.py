#!/usr/bin/env python3
import json
from pathlib import Path
import sys
from subprocess import check_call


def process(fo) -> None:
    j = json.load(fo)
    # todo would be nice to close it here

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

    if p2 is None:
        # handle single file
        if p1 == '-':
            process(fo=sys.stdin)
        else:
            path = Path(p1).absolute()
            with path.open('r') as fo:
                process(fo=fo)
    else:
        assert p1 != '-' and p2 != '-'
        # hacky way to compare
        def cc(p: str):
            if p.endswith('.xz'):
                cat = 'xzcat'
            else:
                cat = 'cat'
            return f'{cat} {p} | {__file__} - | sort'
        c1 = cc(p1)
        c2 = cc(p2)
        cmd = f'vimdiff -c "windo set wrap" <({c1}) <({c2})'
        check_call(cmd, shell=True, executable='/bin/bash')


if __name__ == '__main__':
    main()
