# NOTE: this is experimental for now, best to use the corresponding module bleanser.modules.* instead
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from my.core.cfg import tmp_config

from bleanser.core.modules.extract import ExtractObjectsNormaliser

## disable cache, otherwise it's gonna flush it all the time
# TODO this should be in some sort of common module
os.environ['CACHEW_DISABLE'] = '*'
os.environ.pop('ENLIGHTEN_ENABLE', None)
os.environ['LOGGING_LEVEL_my_twitter_android'] = 'WARNING'  # noqa: SIM112
##

import my.twitter.android as twitter_android


class Normaliser(ExtractObjectsNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def extract_objects(self, path: Path) -> Iterator[Any]:
        class config:
            class twitter:
                class android:
                    export_path = path

        with tmp_config(modules=twitter_android.__name__, config=config):
            assert len(twitter_android.inputs()) == 1  # sanity check to make sure tmp_config worked as expected
            # TODO maybe compare entities? since it's kind of guaranteed to contain everything useful
            for x in twitter_android.bookmarks():
                yield 'bookmark', x
            for x in twitter_android.likes():
                yield 'like', x
            for x in twitter_android.tweets():
                yield 'tweet', x


if __name__ == '__main__':
    Normaliser.main()
