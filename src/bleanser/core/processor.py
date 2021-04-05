from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
from pathlib import Path
from pprint import pprint
import re
from subprocess import DEVNULL
from tempfile import TemporaryDirectory, gettempdir
from typing import Dict, Any, Iterator, Sequence, Optional, Tuple, Optional, Union, Callable, ContextManager, Protocol, List, Set


from .common import CmpResult, Diff, Relation, Group, logger, groups_to_instructions, Keep, Delete, Config
from .utils import DummyExecutor


import more_itertools
from plumbum import local # type: ignore


GREP_FILTER = '> '


Input = Path
Cleaned = Path

class Cleaner(Protocol):
    def __call__(self, path: Input, *, wdir: Path) -> ContextManager[Cleaned]:
        pass


def compute_groups(
        paths: Sequence[Path],
        *,
        cleanup: Cleaner,
        max_workers: Optional[int]=None,
        grep_filter: str,
        config: Config,
        _wdir: Optional[Path]=None,
) -> Iterator[Group]:
    assert len(paths) == len(set(paths)), paths  # just in case

    # if wdir is passed will use this dir instead of a temporary
    # messy but makes debugging a bit easier..
    pool = DummyExecutor() if max_workers == 0 else ThreadPoolExecutor(max_workers=max_workers)
    with pool:
        workers = getattr(pool, '_max_workers')
        morkers = min(workers, len(paths))  # no point in using too many workers
        logger.info('using %d workers', workers)

        chunks = []
        futures = []
        for paths_chunk in more_itertools.divide(workers, paths):
            pp = list(paths_chunk)
            if len(pp) == 0:
                continue
            chunks.append(pp)
            futures.append(pool.submit(
                # force iterator, otherwise it'll still be basically serial
                lambda *args, **kwargs: list(_compute_groups_serial(*args, **kwargs)),
                paths=pp,
                cleanup=cleanup,
                grep_filter=grep_filter,
                config=config,
                _wdir=_wdir,
            ))
        emitted: Set[Path] = set()
        for chunk, f in zip(chunks, futures):
            # TODO maybe not necessary?
            # if last is not None:
            #     # yield fake relation just to fill the gap between chunks...
            #     # TODO kinda annying since it won't be idempotent...
            #     emitted += 1
            #     after = chunk[0]
            #     yield Group(
            #         items =(last, after),
            #         pivots=(last, after),
            #     )
            last = chunk[0]
            rit = f.result()
            for r in rit:
                emitted |= set(r.items)
                yield r
             #    last = r.after
    assert len(emitted) == len(paths), (paths, emitted)  # just in case


diff = local['diff']
grep = local['grep']
cmp_cmd = local['cmp']


# todo these are already normalized paths?
# although then harder to handle exceptions... ugh
def _compute_groups_serial(
        paths: Sequence[Path],
        *,
        cleanup: Cleaner,
        grep_filter: str,
        config: Config,
        _wdir: Optional[Path],
) -> Iterator[Group]:
    assert len(paths) > 0

    CRes = Union[Exception, Cleaned]
    XX = Tuple[Input, CRes]
    XXX = Tuple[XX, XX, XX]

    # TODO just use in-place sort etc?
    # TODO clear intermediate files? (later)
    def xxx() -> Iterator[XX]:
        with ExitStack() as stack:
            wdir: Path
            if _wdir is None:
                wdir = Path(stack.enter_context(TemporaryDirectory()))
            else:
                wdir = _wdir
        for p in paths:
            res: CRes
            try:
                res = stack.enter_context(cleanup(p, wdir=wdir))
            except Exception as e:
                logger.exception(e)
                res = e
            # in theory, it's redundant, but let's tie them together for extra safety...
            yield (p, res)


    alls = list(xxx())

    # TODO dominated here could tell apart proper subsets...
    def isfsubset(left: Sequence[Path], right: Sequence[Path]) -> bool:
        def toset(idxs: Sequence[Path]) -> Set[str]:
            res = set()
            for i in idxs:
                res |= set(i.read_text().splitlines())
            return res
        if config.multiway:
            return toset(left) <= toset(right)
        else:
            # TODO ugh. total crap
            for s1, s2 in zip(left, left[1:]):
                if not toset([s1]) <= toset([s2]):
                    return False
            return True


    def issubset(left: List[int], right: List[int]) -> bool:
        return isfsubset([paths[i] for i in left], [paths[i] for i in right])


    def lunique(l: List[Path]) -> List[Path]:
        return list(more_itertools.unique_everseen(l))

    left  = 0
    while left < len(alls):
        items  = [paths[left]]
        lpivot = left
        rpivot = left
        # invaraint
        # - items, lpivot, rpivot are all valid
        # - sets corresponding to lpivot + rpivot contain all of 'items'
        # next we attempt to
        # - rpivot: hopefully advance as much as possible
        # - items : expand to include as much as possible

        right = left + 1
        while True:
            pivots = lunique([paths[lpivot], paths[rpivot]])
            if right == len(alls):
                # end of sequence, so the whole tail is in the same group
                g = Group(
                    items =items,
                    pivots=pivots,
                )
                yield g
                left = len(alls)
                break
            else:
                # try to advance right while maintaining invariants
                nitems  = lunique(items + [paths[right - 1], paths[right]])
                npivots = lunique([paths[lpivot], paths[right]])
                dominated = isfsubset(nitems, npivots)

                if not dominated:
                    # yield the last good result
                    g = Group(
                        items =items,
                        pivots=pivots,
                    )
                    yield g
                    # TODO eh. a bit crap, but seems that a special case is necessary
                    if len(pivots) == 2:
                        left = rpivot
                    else:
                        left = rpivot + 1
                    break
                else:
                    # advance it
                    items  = nitems
                    # lpivot is unchanged
                    rpivot = right
                    right += 1
    return

    # FIXME need this code to cleanup old stuff!!
    def outputs() -> Iterator[XXX]:
        with ExitStack() as stack:
            wdir: Path
            if _wdir is None:
                wdir = Path(stack.enter_context(TemporaryDirectory()))
            else:
                wdir = _wdir

            paths.append(Path('dummy'))  # TODO
            cur: List[XX] = []
            for cp in paths:
                res: Union[Exception, Cleaned]
                try:
                    res = stack.enter_context(cleanup(cp, wdir=wdir))
                except Exception as e:
                    logger.exception(e)
                    res = e
                next_ = (cp, res)

                assert len(cur) <= 3, cur  # just in case
                if len(cur) == 3:
                    old = cur[0]
                    old_input, old_res = old
                    if not isinstance(old_res, Exception):
                        # meh. unlink is a bit manual, but bounds the filesystem use by two dumps
                        # handle 'identity' cleanup -- shouldn't try to remove user files
                        if old_res != old_input:
                            # meh... jus in case
                            assert str(old_res).startswith(gettempdir()), old_res
                            old_res.unlink()  # todo no need to unlink in debug mode
                    cur = cur[1:]

                cur.append(next_)
                if len(cur) == 3:
                    yield tuple(cur)  # type: ignore[misc]

    # TODO later, migrate core to use it?
    # diffing/relation generation can be generic
    #
    # TODO outputs should go one by one... zipping should be separate perhaps?
    # also we might want to retain intermediate... ugh. mindfield

    for [(p1, dump1), (p2, dump2), (p3, dump3)] in outputs():
        # ok, so two way comparison is like the three way one, but where the last file is always empty?
        # so could

        logger.info("cleanup: %s vs %s", p1, p2)
        # todo would be nice to dump relation result?
        # TODO could also use sort + comm? not sure...
        # sorting might be a good idea actually... would work better with triples?

        def rel(*, before: Path, after: Path, diff: Diff) -> Relation:
            logger.debug('%s vs %s: %s', before, after, diff.cmp)
            return Relation(before=before, after=after, diff=diff)

        if isinstance(dump1, Exception) or isinstance(dump2, Exception):
            yield rel(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.ERROR))
            continue

        # just for mypy...
        assert isinstance(dump1, Path), dump1
        assert isinstance(dump2, Path), dump2

        # first check if they are identical (should be super fast, stops at first byte difference)
        (rc, _, _) = cmp_cmd['--silent', dump1, dump2].run(retcode=(0, 1))
        if rc == 0:
            yield rel(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.SAME))
            continue

        # print(diff[dump1, dump2](retcode=(0, 1)))  # for debug
        cmd = diff[dump1, dump2]  | grep['-vE', grep_filter]
        res = cmd(retcode=(0, 1))
        if len(res) > 10000:  # fast track to fail
            # TODO Meh
            yield rel(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DIFFERENT))
            continue
        rem = res.splitlines()
        # clean up diff crap like
        # 756587a756588,762590
        rem = [l for l in rem if not re.fullmatch(r'\d+a\d+(,\d+)?', l)]
        if len(rem) == 0:
            yield rel(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DOMINATES))
        else:
            # TODO maybe log verbose differences to a file?
            yield rel(before=p1, after=p2, diff=Diff(diff=b'', cmp=CmpResult.DIFFERENT))


# note: also some tests in sqlite.py

def test_bounded_resources(tmp_path: Path) -> None:
    """
    Check that relation processing is iterative in terms of not using too much disk space for temporary files
    """

    one_mb = 1_000_000
    text = 'x' * one_mb + '\n'


    idir = tmp_path / 'idir'
    wdir = tmp_path / 'wdir'
    idir.mkdir()
    wdir.mkdir()

    # each file would be approx 1mb in size
    inputs = []
    for i in range(10):
        ip = idir / f'{i}.txt'
        text += '\n' + str(i) + '\n'
        ip.write_text(text)
        inputs.append(ip)
    ##

    @contextmanager
    def dummy(path: Path, *, wdir: Path) -> Iterator[Path]:
        tp = wdir / (path.name + '-dump')
        tp.write_text(path.read_text())
        yield tp

    config = Config()
    func = lambda paths: compute_groups(paths, cleanup=dummy, max_workers=0, grep_filter=GREP_FILTER, config=config, _wdir=wdir)

    from .utils import total_dir_size

    for i, r in enumerate(func(inputs)):
        ds = total_dir_size(wdir)
        # at no point should use more than 3 dumps... + some leeway
        assert ds < 4 * one_mb, ds
        # TODO!
        # assert r.diff.cmp == CmpResult.DOMINATES

        if i > 3:
            # in 'steady' mode should take some space? more of a sanity check..
            assert ds > one_mb, ds


@contextmanager
def _noop(path: Path, *, wdir: Path) -> Iterator[Path]:
    yield path


def test_simple(tmp_path: Path) -> None:
    config = Config()

    path1 = tmp_path / 'path1'
    path2 = tmp_path / 'path2'
    path3 = tmp_path / 'path3'

    path1.write_text('A\n')
    path2.write_text('B\n')
    path3.write_text('C\n')

    for gg in [
            [path1],
            [path1, path2],
            [path1, path2, path3],
    ]:
        groups = list(compute_groups(
            gg,
            cleanup=_noop, max_workers=0, config=config, grep_filter=GREP_FILTER,
        ))
        instructions = groups_to_instructions(groups, config=config)
        assert [type(i) for i in instructions] == [Keep for _ in gg]


def _prepare(tmp_path: Path):
    sets = [
        ['X'],                # keep
        ['B'],                # delete
        ['B', 'A'],           # delete
        ['C', 'B', 'A'],      # keep
        ['A', 'BB', 'C'],     # keep
        ['B', 'A', 'E', 'Y'], # keep
    ]

    paths = []
    for i, s  in enumerate(sets):
        o = tmp_path / f'{i}.txt'
        # TODO ugh. how to get rid of \\ No newline at end of file ??
        o.write_text('\n'.join(s) + '\n')
        paths.append(o)
    return paths


def test_twoway(tmp_path: Path) -> None:
    paths = _prepare(tmp_path)

    config = Config(delete_dominated=True, multiway=False)
    groups = list(compute_groups(paths, cleanup=_noop, max_workers=0, config=config, grep_filter=GREP_FILTER))
    instructions = groups_to_instructions(groups, config=config)
    assert [type(i) for i in instructions] == [
        Keep,
        Keep,
        Delete,  # dominated by the next set
        Keep,
        Keep,
        Keep
    ]

    for p in paths:
        assert p.exists(), p  # just in case


def test_multiway(tmp_path: Path) -> None:
    # TODO test multi way against old bluemaestro dbs?
    paths = _prepare(tmp_path)
    for i, s in enumerate([
            ['00', '11', '22'],
            ['11', '22', '33', '44'],
            ['22', '33', '44', '55'],
            ['44', '55', '66'],
            ['55', '66'],
    ]):
        p = tmp_path / f'extra_{i}.txt'
        p.write_text('\n'.join(s) + '\n')
        paths.append(p)

    # TODO grep filter goes into the config?
    config = Config(
        delete_dominated=True,
        multiway=True,
    )
    groups = list(compute_groups(paths, cleanup=_noop, max_workers=0, grep_filter=GREP_FILTER, config=config))
    instructions = groups_to_instructions(groups, config=config)

    assert [type(i) for i in instructions] == [
        Keep,    # X
        Delete,  # B  in CBA
        Delete,  # BA in CBA
        Keep,    # keep CBA
        Keep,    # keep because of BB
        Keep,    # Keep because of E,Y
        # extra items now
        Keep,
        Delete,  #
        Keep  ,  # in isolation, it's dominated by neighbours.. but if we delete it, we'll lose '33' permanently
        Delete,  # dominated by neighbours
        Keep  ,  # always keep last
    ]
    # for i in instructions:
    #     print(i)


# two way -- if we have a sequence of increasing things, then it's just a single partition?
