from bleanser.core.modules.json import Json, JsonNormaliser, delkeys


class Normaliser(JsonNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, j: Json) -> Json:
        delkeys(
            j,
            keys=[
                ## these are change all the time, and I guess if you were interested in any 'real time' dynamics
                ## you wouldn't use periodic backups anyway, just write a proper polling tool
                ## especially considering they are cumulative, fine to prune out
                'reputation',
                'view_count',
                'favorite_count',
                'up_vote_count',
                'down_vote_count',
                'answer_count',
                'score',
                ##
                ##
                'reputation_change_week',
                'reputation_change_month',
                'reputation_change_quarter',
                'reputation_change_year',
                'profile_image',
                'last_access_date',  # last time user loggen in? very flaky
            ],
        )

        ##
        # the json maps from 'domain' (e.g. math/english/apple) to the payload with various interesting data
        # so we wanna flatten it first
        nj = {}
        for domain, d in j.items():
            for k, v in d.items():
                nj[f'{domain}_{k}'] = v
        j = nj
        ##

        ##
        for k in list(j.keys()):
            if k.endswith('/privileges'):  # useless crap, achievements/badges
                del j[k]
        ##
        return j


if __name__ == '__main__':
    Normaliser.main()
