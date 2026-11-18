import os
from pathlib import Path
from typing import Any, Iterator

from bleanser.core.modules.extract import ExtractObjectsNormaliser

from my.core.cfg import tmp_config
from my.core.freezer import Freezer


# disable cache, otherwise it's gonna flush it all the time
os.environ['CACHEW_DISABLE'] = '*'
os.environ.pop('ENLIGHTEN_ENABLE', None)
os.environ['LOGGING_LEVEL_rexport_dal'] = 'WARNING'
# os.environ['LOGGING_LEVEL_my_reddit_rexport'] = 'WARNING'

import my.reddit.rexport as reddit


class Normaliser(ExtractObjectsNormaliser):
    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class reddit:
                # FIXME need to put in reddit.rexport
                export_path = path

        with tmp_config(modules=reddit.__name__, config=config):
            ## sanity check to make sure tmp_config worked as expected
            # for most modules should be able to use module.inputs() directly though
            assert len(reddit.inputs()) == 1

            reddit_profile = lambda: [reddit.profile()]
            for (method, type_) in [
                # fmt: off
                (reddit.saved       , reddit.Save       ),
                (reddit.comments    , reddit.Comment    ),
                (reddit.submissions , reddit.Submission ),
                (reddit.upvoted     , reddit.Upvote     ),
                (reddit.subreddits  , reddit.Subreddit  ),
                (reddit.multireddits, reddit.Multireddit),
                (reddit_profile     , reddit.Profile    ),
                # fmt: on
            ]:
                # need to run it past freezer so it's dumped as dataclass
                freezer = Freezer(Orig=type_)
                for x in map(freezer.freeze, method()):
                    # raw data might be too noisy
                    x.raw = None  # type: ignore
                    # FIXME currently freezer hardcodes RRR for dataclass name
                    yield {type_.__name__: x}


if __name__ == '__main__':
    Normaliser.main()
