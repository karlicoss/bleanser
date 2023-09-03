#!/usr/bin/env python3
from bleanser.modules.json_new import JsonNormaliser, Json


class Normaliser(JsonNormaliser):
    PRUNE_DOMINATED = True
    MULTIWAY = True

    def cleanup(self, j: Json) -> Json:
        if isinstance(j, list):
            # old format -- I think only contained events log or something
            return j

        for what in ['watched', 'starred', 'subscriptions']:
            for r in j[what]:
                # these are gonna be super flaky, so just ignore from diff
                for k in [
                        'stargazers_count',
                        'watchers',
                        'watchers_count',
                        'updated_at',
                        'pushed_at',
                        'forks',
                        'forks_count',
                        'open_issues',
                        'open_issues_count',
                        'size',
                ]:
                    r.pop(k, None)

        for r in j['repos']:
            ## need to 'flatten' traffic, otherwise it can't properly figure out diffs
            ## TODO possible to make generic, e.g. hint the normaliser that we need to flatten .repos.traffic.clones field
            traffic = r.get('traffic')
            if traffic is None:
                continue
            for key in ['clones', 'views']:
                xxx = traffic[key]
                xxx.pop('count')   # aggregate
                xxx.pop('uniques') # aggregate
                assert xxx.keys() == {key}
                # NOTE: we ignore first and last traffic entry since timestamps are aligned to the closest day
                # so they are always going to be kinda flaky
                for c in xxx[key][1: -1]:
                    ts = c['timestamp']
                    j[f'{r["full_name"]}_traffic_{key}_{ts}'] = c
                xxx.pop(key)

        # TODO should probably prefer in place cleanup to make consistent with sqlite? not sure
        return j


if __name__ == '__main__':
    Normaliser.main()
