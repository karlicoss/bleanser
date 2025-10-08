from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import orjson

from bleanser.core.processor import (
    BaseNormaliser,
    Normalised,
    sort_file,
    unique_file_in_tempdir,
)

# imports for convenience -- they are used in other modules
from bleanser.core.utils import Json, delkeys, patch_atoms  # noqa: F401


class JsonNormaliser(BaseNormaliser):
    PRUNE_DOMINATED = False

    def cleanup(self, j: Json) -> Json:
        '''
        subclasses should override this function, to do the actual cleanup

        cleanup in this context means removing extra JSON keys which are not
        needed to produce a normalised representation for a file
        '''
        return j

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Normalised]:
        # TODO maybe, later implement some sort of class variable instead of hardcoding
        # note: deliberately keeping mime check inside do_cleanup, since it's executed in a parallel process
        # otherwise it essentially blocks waiting for all mimes to compute..
        # TODO crap. annoying, sometimes mime determines as text/plain for no reason
        # I guess doesn't matter as much, json.loads is the ultimate check it's ineed json
        # mp = mime(upath)
        # assert mp in {
        #         'application/json',
        # }, mp

        j = orjson.loads(path.read_text())
        j = self.cleanup(j)

        # create a tempfile to write flattened data to
        cleaned = unique_file_in_tempdir(input_filepath=path, dir=self.tmp_dir, suffix='.json')

        with cleaned.open('w') as fo:
            if isinstance(j, list):
                j = {'<toplevel>': j}  # meh

            assert isinstance(j, dict), j
            for k, v in j.items():
                if not isinstance(v, list):
                    # something like 'profile' data in hypothesis could be a dict
                    # something like 'notes' in rescuetime could be a scalar (str)
                    v = [v]  # meh
                assert isinstance(v, list), (k, v)
                for i in v:
                    print(f'{k} ::: {orjson.dumps(i, option=orjson.OPT_SORT_KEYS).decode("utf8")}', file=fo)

        # todo meh... see Fileset._union
        # this gives it a bit of a speedup, just calls out to unix sort
        sort_file(cleaned)

        yield cleaned


if __name__ == '__main__':
    JsonNormaliser.main()


# TODO actually implement some artificial json test
#
def test_nonidempotence(tmp_path: Path) -> None:
    from bleanser.tests.common import actions, hack_attribute

    '''
    Just demonstrates that multiway processing might be
    It's probably going to be very hard to fix, likely finding 'minimal' cover (at least in terms of partial ordering) is NP hard?
    '''

    # fmt: off
    sets = [
        [],
        ['a'],
        ['a', 'b'],
        [     'b', 'c'],
        ['a', 'b', 'c'],
    ]
    # fmt: on
    for i, s in enumerate(sets):
        p = tmp_path / f'{i}.json'
        p.write_text(orjson.dumps(s).decode('utf8'))

    with (
        hack_attribute(JsonNormaliser, 'MULTIWAY', value=True),
        hack_attribute(JsonNormaliser, 'PRUNE_DOMINATED', value=True),
    ):
        paths = sorted(tmp_path.glob('*.json'))
        res = actions(paths=paths, Normaliser=JsonNormaliser)

        assert [p.name for p in res.remaining] == [
            '0.json',  # keeping as boundary
            '2.json',  # keeping because item a has rolled over
            '4.json',  # keeping as boundary
        ]

        paths = list(res.remaining)
        res = actions(paths=paths, Normaliser=JsonNormaliser)
        assert [p.name for p in res.remaining] == [
            '0.json',
            # note: 2.json is removed because fully contained in 4.json
            '4.json',
        ]
