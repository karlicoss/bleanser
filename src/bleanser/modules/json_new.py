#!/usr/bin/env python3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
import json


from bleanser.core.json import delkeys
from bleanser.core.processor import BaseNormaliser
from bleanser.core.utils import Json


class Normaliser(BaseNormaliser):
    # filter out additions; keep the rest
    DIFF_FILTER =  '> '

    def cleanup(self, j: Json) -> Json:
        keys = {
               'subreddit_subscribers',
               'subscribers',
               'ups',
               'score',
               'num_comments',

               'upvote_ratio',

               'videostream_links_count',
               'dash_url',
               'hls_url',
               'post_hint',
               'author_premium',
               'thumbnail_height',
               'thumbnail_width',

               'author_patreon_flair',
               'author_flair_text_color',

               'parent_whitelist_status', # some ads thing
               'whitelist_status', # some ads thing
               'wls', # TODO ???

               # 'secure_media', # flaky, not sure why. not important, ther is another media link

               # 'giver_coin_rewrad',
               'all_awardings',

               'pwls', # TODO what is it??
               'likes', # todo?
        }
        delkeys(j, keys=keys)
        return j

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        with self.unpacked(path=path, wdir=wdir) as upath:
            pass
        del path # just to prevent from using by accident

        j = json.loads(upath.read_text())
        j = self.cleanup(j)

        # todo copy paste from SqliteNormaliser
        jpath = upath.absolute().resolve()
        cleaned = wdir / Path(*jpath.parts[1:]) / (jpath.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)

        with cleaned.open('w') as fo:
            if isinstance(j, list):
                j = {'<toplevel>': j} # meh

            assert isinstance(j, dict), j
            for k, v in j.items():
                if not isinstance(v, list):
                    # something like 'profile' data in hypothesis could be a dict
                    # something like 'notes' in rescuetime could be a scalar (str)
                    v = [v] # meh
                assert isinstance(v, list), (k, v)
                for i in v:
                    print(f'{k} ::: {json.dumps(i, sort_keys=True)}', file=fo)

        yield cleaned


if __name__ == '__main__':
    Normaliser.main()
