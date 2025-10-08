from bleanser.core.modules.json import Json, JsonNormaliser


class Normaliser(JsonNormaliser):
    PRUNE_DOMINATED = True
    MULTIWAY = True

    def cleanup(self, j: Json) -> Json:
        if isinstance(j, list):
            # old format -- I think only contained events log or something
            return j

        profile = j.get('profile')
        if profile is not None:
            profile.pop('disk_usage', None)
            profile.pop('updated_at', None)  # I think it updates at any github activity, so pretty pointless

            # pretty volatile, so not worth keeping + reflected in "followers" field anyway
            profile.pop('followers', None)

        volatile = [
            'stargazers_count',
            'watchers',
            'watchers_count',
            'forks',
            'forks_count',
            'open_issues',
            'open_issues_count',
        ]

        for what in ['repos', 'watched', 'starred', 'subscriptions']:
            thing = j.get(what)
            if thing is None:
                continue
            for r in thing:
                # these are gonna be super flaky, so just ignore from diff
                # for our own repos they are duplicated in events anyway
                for k in [
                    *volatile,
                    'updated_at',
                    'pushed_at',
                    'size',
                ]:
                    r.pop(k, None)

                repo_name = r["full_name"]
                if repo_name == 'emacs-straight/advice-patch':
                    r.pop('description')
                    # changes every day automatically
                    # TODO move to private overlay?

        for r in j['repos']:
            repo_name = r["full_name"]

            for k in volatile:
                v = r.get(k)
                if v is None:
                    continue
                r[k] = r[k] // 10 * 10  # round up to nearest multiple of 10 so there are less diffs

            ## need to 'flatten' traffic, otherwise it can't properly figure out diffs
            ## TODO possible to make generic, e.g. hint the normaliser that we need to flatten .repos.traffic.clones field
            traffic = r.get('traffic')
            if traffic is None:
                continue
            for key in ['clones', 'views']:
                xxx = traffic[key]
                xxx.pop('count')  # aggregate
                xxx.pop('uniques')  # aggregate
                assert xxx.keys() == {key}
                # NOTE: we ignore first and last traffic entry since timestamps are aligned to the closest day
                # so they are always going to be kinda flaky
                for c in xxx[key][1:-1]:
                    ts = c['timestamp']
                    j[f'{repo_name}_traffic_{key}_{ts}'] = c
                xxx.pop(key)
            for key in ['popular/paths', 'popular/referrers']:
                # TODO hmm these are still quite flaky? they collect stats over last two weeks so can change a lot..
                j[f'{repo_name}_traffic_{key}'] = traffic[key]
                traffic.pop(key)

        # TODO should probably prefer in place cleanup to make consistent with sqlite? not sure
        return j


if __name__ == '__main__':
    Normaliser.main()
