from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

import pytest

from ..common import Group, Keep, Prune
from ..processor import _FILTER_ALL_ADDED, BaseNormaliser, FileSet, Normalised, compute_groups, groups_to_instructions
from ..utils import total_dir_size


def test_fileset(tmp_path: Path) -> None:
    wdir = tmp_path / 'wdir'
    wdir.mkdir()

    FS = lambda *paths: FileSet(paths, wdir=wdir)

    fid = 0

    def lines(ss) -> Path:
        nonlocal fid
        f = tmp_path / str(fid)
        f.write_text(''.join(s + '\n' for s in ss))
        fid += 1
        return f

    dfilter = _FILTER_ALL_ADDED
    f1 = lines([])
    fs_ = FS(f1)
    f2 = lines([])
    assert FS(f1).issubset(FS(f2), diff_filter=dfilter)

    assert FS(f1).issame(FS(f2))

    fsac = FS(lines(['a', 'c']))

    # fmt: off
    assert     fsac.issame(FS(lines(['a', 'c'])))
    assert not fsac.issame(FS(lines(['a', 'c', 'b'])))
    assert not FS(lines(['a', 'c', 'b'])).issame(fsac)

    assert     fs_ .issubset(fsac, diff_filter=dfilter)
    assert not fsac.issubset(fs_ , diff_filter=dfilter)
    assert     fsac.issubset(fs_ , diff_filter='.*')
    # fmt: on

    fc = lines(['c'])
    fe = lines(['e'])
    fsce = FS(fc, fe)
    assert not fsce.issubset(fsac, diff_filter=_FILTER_ALL_ADDED)
    assert not fsac.issubset(fsce, diff_filter=_FILTER_ALL_ADDED)

    fa = lines(['a'])
    fscea = fsce.union(fa)
    assert fsce.issubset(fscea, diff_filter=_FILTER_ALL_ADDED)


@pytest.mark.parametrize(
    ('multiway', 'randomize'),
    [
        (False, False),
        (True , False),
        (True , True),
        (False, True),
    ],
)  # fmt: skip
def test_bounded_resources(*, tmp_path: Path, multiway: bool, randomize: bool) -> None:
    """
    Check that relation processing is iterative in terms of not using too much disk space for temporary files
    """
    # max size of each file
    one_mb = 1_000_000
    text = 'x' * one_mb + '\n'

    idir = tmp_path / 'inputs'
    idir.mkdir()

    import string
    from random import Random

    r = Random(0)
    # each file would be approx 1mb in size
    inputs = []
    for g in range(4):  # 4 groups
        for i in range(20):  # 20 backups in each group
            ip = idir / f'{g}_{i}.txt'
            text += str(i) + '\n'
            extra = r.choice(string.printable) + '\n' if randomize else ''
            ip.write_text(text + extra)
            inputs.append(ip)
        ip = idir / f'{g}_sep.txt'
        ip.write_text('GARBAGE')
        inputs.append(ip)
    ##

    idx = 0
    tmp_dir_spaces = []

    def check_tmp_dir_space(tmp_dir: Path) -> None:
        nonlocal idx
        # logger.warning('ITERATION: %s', idx)
        ds = total_dir_size(tmp_dir)

        # 7 is a bit much... but currently it is what it is, can be tighter later
        # basically
        # - at every point we keep both pivots (2 x 1mb)
        # - we keep the merged bit (about 1mb in this specific test cause of overlap)
        # - we keep one next file (1mb)
        # - we might need to copy the merged bit at some point as well to test it as a candidate for next
        threshold = 7 * one_mb
        # check_call(['ls', '-al', gwdir])

        if ds > threshold:
            # raise BaseException, so it propagates all the way up and doesn't trigget defensive logic
            raise BaseException("working dir takes too much space")  # noqa: TRY002

        tmp_dir_spaces.append(ds)
        idx += 1

    class TestNormaliser(BaseNormaliser):
        MULTIWAY = multiway
        PRUNE_DOMINATED = True

        @contextmanager
        def normalise(self, *, path: Path) -> Iterator[Normalised]:
            normalised = self.tmp_dir / 'normalised'
            normalised.write_text(path.read_text())
            # ugh. it's the only place we can hook in to do frequent checks..
            check_tmp_dir_space(self._base_tmp_dir)
            yield normalised

    func = lambda paths: compute_groups(paths, Normaliser=TestNormaliser)

    # force it to compute
    groups = list(func(inputs))
    # if all good, should remove all the intermediate ones?
    # so
    # - in twoway   mode: 4 seps + 2 boundary files in each group = 12 groups
    # - in multiway mode: seps end up as part of groups, so it's just 8 groups
    # if it goes bad, there will be error on each step
    if randomize:
        assert len(groups) > 40
    else:
        expected = 8 if multiway else 12
        assert len(groups) == expected

    # check working dir spaces
    # in 'steady' mode should take some space? more of a sanity check..
    took_space = len([x for x in tmp_dir_spaces if x > one_mb])
    assert took_space > 20


@pytest.mark.parametrize('multiway', [False, True])
def test_many_files(*, tmp_path: Path, multiway: bool) -> None:
    N = 2000

    # BaseNormaliser is just emitting original file by default, which is what we want here
    class TestNormaliser(BaseNormaliser):
        MULTIWAY = multiway
        PRUNE_DOMINATED = True

    paths = []
    for i in range(N):
        p = tmp_path / f'{i:05}'
        paths.append(p)
        p.write_text(str(i % 10 > 5) + '\n')

    groups = list(compute_groups(paths, Normaliser=TestNormaliser))

    # shouldn't crash due to open files or something, at least
    expected = 399 if multiway else 799
    assert len(groups) == expected


def test_special_characters(tmp_path: Path) -> None:
    class TestNormaliser(BaseNormaliser):
        MULTIWAY = True
        PRUNE_DOMINATED = True

    p1 = tmp_path / 'p1'
    p1.write_text('A\n')
    p2 = tmp_path / 'p2'
    p2.write_text('A\n< C > whoops\n')
    p3 = tmp_path / 'p3'
    p3.write_text('A\n< C > whoops\n')
    p4 = tmp_path / 'p4'
    p4.write_text('A\n')

    gg = [p1, p2, p3, p4]
    groups = list(compute_groups(gg, Normaliser=TestNormaliser))
    instructions = groups_to_instructions(groups)
    assert [type(i) for i in instructions] == [
        Keep,   # start of group
        Prune,  # same as next
        Keep,   # has unique item: < C > whoops
        Keep,   # end of group
    ]  # fmt: skip


@pytest.mark.parametrize('multiway', [False, True])
def test_simple(*, tmp_path: Path, multiway: bool) -> None:
    class TestNormaliser(BaseNormaliser):
        PRUNE_DOMINATED = True
        MULTIWAY = multiway

    p1 = tmp_path / 'p1'
    p2 = tmp_path / 'p2'
    p3 = tmp_path / 'p3'
    p4 = tmp_path / 'p4'

    p1.write_text('A\n')
    p2.write_text('B\n')
    p3.write_text('C\n')
    p4.write_text('D\n')

    for gg in [
        [p1],
        [p1, p2],
        [p1, p2, p3],
        [p1, p2, p3, p4],
    ]:
        groups = list(compute_groups(gg, Normaliser=TestNormaliser))
        instructions = groups_to_instructions(groups)
        assert [type(i) for i in instructions] == [Keep for _ in gg]


def test_filter(tmp_path: Path) -> None:
    class TestNormaliser(BaseNormaliser):
        PRUNE_DOMINATED = False
        MULTIWAY = False

        @contextmanager
        def normalise(self, *, path: Path) -> Iterator[Normalised]:
            normalised = self.tmp_dir / 'normalised'
            with path.open('r') as fr, normalised.open('w') as fw:
                for line in fr:
                    # drop all lines except "A"
                    if line == 'A\n':
                        fw.write(line)
            yield normalised

    p1 = tmp_path / 'p1'
    p2 = tmp_path / 'p2'
    p3 = tmp_path / 'p3'
    p4 = tmp_path / 'p4'
    paths = [p1, p2, p3, p4]

    ## p1, p2 and p3 are same as long as the filter concerned
    ## NOTE: p2 is the same because unique lines are ignored? this is a bit confusing here..
    p1.write_text('b\nA\nc\n')
    p2.write_text('A\nx\nA\nu\n')
    p3.write_text('A\nd\n')
    p4.write_text('x\ny\n')

    groups = list(compute_groups(paths, Normaliser=TestNormaliser))
    instructions = groups_to_instructions(groups)
    assert [type(i) for i in instructions] == [
        Keep,
        Prune,  # should prune because after filtering only A there is no difference in files
        Keep,
        Keep,
    ]


def _prepare_testdata(tmp_path: Path) -> list[Path]:
    sets = [
        ['X'],                # keep
        ['B'],                # keep
        ['B'],                # prune (always, because it's the same)
        ['B'],                # prune if we prune dominated
        ['B', 'A'],           # prune if we prune dominated
        ['C', 'B', 'A'],      # keep
        ['A', 'BB', 'C'],     # keep
        ['B', 'A', 'E', 'Y'], # keep
    ]  # fmt: skip

    paths = []
    for i, s in enumerate(sets):
        o = tmp_path / f'{i}.txt'
        # TODO ugh. how to get rid of \\ No newline at end of file ??
        o.write_text('\n'.join(s) + '\n')
        paths.append(o)
    return paths


@pytest.mark.parametrize(
    'prune_dominated',
    [
        True,
        False,
    ],
)
def test_twoway(*, tmp_path: Path, prune_dominated: bool) -> None:
    paths = _prepare_testdata(tmp_path)

    class TestNormaliser(BaseNormaliser):
        PRUNE_DOMINATED = prune_dominated
        MULTIWAY = False

    groups = list(
        compute_groups(
            paths,
            Normaliser=TestNormaliser,
        )
    )
    instructions = list(groups_to_instructions(groups))
    assert [type(i) for i in instructions] == [
        Keep,
        Keep,
        Prune,
        Prune if prune_dominated else Keep,  # dominated
        Prune if prune_dominated else Keep,  # dominated by the next set
        Keep,
        Keep,
        Keep,
    ]

    for p in paths:
        assert p.exists(), p  # just in case


# TODO test multi way against old bluemaestro dbs?
def test_multiway(tmp_path: Path) -> None:
    paths = _prepare_testdata(tmp_path)

    class TestNormaliser(BaseNormaliser):
        PRUNE_DOMINATED = True
        MULTIWAY = True

    for i, s in enumerate(
        [
            ['00', '11', '22'],
            ['11', '22', '33', '44'],
            ['22', '33', '44', '55'],
            ['44', '55', '66'],
            ['55', '66'],
        ]
    ):
        p = tmp_path / f'extra_{i}.txt'
        p.write_text('\n'.join(s) + '\n')
        paths.append(p)

    groups = list(
        compute_groups(
            paths,
            Normaliser=TestNormaliser,
        )
    )
    instructions = groups_to_instructions(groups)

    assert [type(i) for i in instructions] == [
        Keep,    # X
        Prune,   # B in CBA
        Prune,   # B in CBA
        Prune,   # B in CBA
        Prune,   # BA in CBA
        Keep,    # keep CBA
        Keep,    # keep because of BB
        Keep,    # Keep because of E,Y
        # extra items now
        Keep,
        Prune,   #
        Keep,    # in isolation, it's dominated by neighbours.. but if we prune it, we'll lose '33' permanently
        Prune,   # dominated by neighbours
        Keep,    # always keep last
    ]  # fmt: skip


def test_groups_to_instructions() -> None:
    def do(*pp: Sequence[str]):
        ppp = [list(map(Path, s)) for s in pp]
        # for this test we assume pivots are just at the edges
        grit = (
            Group(
                items=p,
                pivots=(p[0], p[-1]),
                error=False,
            )
            for p in ppp
        )
        res = groups_to_instructions(list(grit))
        return [(str(p.path), {Keep: 'keep', Prune: 'prune'}[type(p)]) for p in res]

    assert do(
        ('a', 'b'),
    ) == [
        ('a', 'keep'),
        ('b', 'keep'),
    ]

    assert do(
        ('0', 'a'),
        ('a', 'b', 'c', 'd'),
    ) == [
        ('0', 'keep'),
        ('a', 'keep'),
        ('b', 'prune'),
        ('c', 'prune'),
        ('d', 'keep'),
    ]

    # TODO shit. how to test this now?
    # maybe it's the config -- delete both pivots or not? not sure
    # inputs = [
    #    ('a', 'b', CR.SAME     ),
    #    ('b', 'c', CR.DIFFERENT),
    #    ('c', 'd', CR.DOMINATES),
    #    ('d', 'e', CR.SAME     ),
    #    ('e', 'f', CR.DOMINATES),
    #    ('f', 'g', CR.DIFFERENT),
    #    ('g', 'h', CR.SAME     ),
    # ]
    #
    # assert do(*inputs) == [
    #    ('a', 'keep'  ),
    #    ('b', 'keep'  ),
    #    ('c', 'keep'  ),
    #    ('d', 'keep'  ),
    #    ('e', 'keep'  ),
    #    ('f', 'keep'  ),
    #    ('g', 'keep'  ),
    #    ('h', 'keep'  ),
    # ]
    #
    # assert do(*inputs, config=Config(prune_dominated=True)) == [
    #    ('a', 'keep'  ),
    #    ('b', 'keep'  ),
    #    ('c', 'keep'  ),
    #    ('d', 'prune' ),
    #    ('e', 'prune' ),
    #    ('f', 'keep'  ),
    #    ('g', 'keep'  ),
    #    ('h', 'keep'  ),
    # ]

    with pytest.raises(RuntimeError, match='duplicate items'):
        # x appears twice in the same group
        do(
            ('a', 'b'),
            ('b', 'x', 'y', 'x', 'd'),
            ('d', 'e'),
        )

    with pytest.raises(RuntimeError, match='multiple groups'):
        # b is duplicate
        do(
            ('a', 'b', 'c'),
            ('c', 'x', 'y', 'b', 'e'),
        )

    with pytest.raises(RuntimeError, match='pivot and non-pivot'):
        # b is uses both a pivot and non-pivot
        do(
            ('x', 'y', 'a'),
            ('a', 'b', 'c'),
            ('b', 'a'),
        )

    # # TODO not sure if should raise... no pivot overlap?
    # with pytest.raises(AssertionError):
    #     do(
    #         ('a', 'b'),
    #         ('c', 'd'),
    #     )


# note: also some tests in sqlite.py
