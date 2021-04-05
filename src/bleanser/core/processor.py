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
            last = chunk[0]
            rit = f.result()
            for r in rit:
                emitted |= set(r.items)
                yield r
    assert emitted == set(paths), (paths, emitted)  # just in case


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

    # FIXME ugh. need to make it properly iterative...
    stack = ExitStack()

    # TODO clear intermediate files? (later)
    def xxx() -> Iterator[XX]:
        # with ExitStack() as stack:
        if True:
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


    xxx_res = list(xxx())

    assert len(xxx_res) == len(paths)
    for ip, (ip2, cl) in zip(paths, xxx_res):
        assert ip == ip2, (ip, ip2)

    cleaned2orig = {c: i for i, c in xxx_res}
    cleaned = list(cleaned2orig.keys())

    # delete it so we don't try to use it later by accident
    del paths


    # TODO extract & test separately?
    def _isfsubset(lefte: Sequence[CRes], righte: Sequence[CRes]) -> bool:
        lefts  = []
        for i in lefte:
            if isinstance(i, Exception):
                return False
            else:
                lefts.append(i)
        rights = []
        for i in righte:
            if isinstance(i, Exception):
                return False
            else:
                rights.append(i)

        # TODO just use in-place sort etc?
        cat = local['cat']
        sort = local['sort']
        # TODO short circuit if files are subsets as sets?
        with TemporaryDirectory() as td:
            tdir = Path(td)
            lfile = tdir / 'left'
            rfile = tdir / 'right'
            (cat | sort['--unique'] > str(lfile))(*lefts)
            (cat | sort['--unique'] > str(rfile))(*rights)

            # TODO tbh shoudl just use cmp/comm for the rest... considering it's all sorted
            # first check if they are identical (should be super fast, stops at the first byte difference)
            (rc, _, _) = cmp_cmd['--silent', lfile, rfile].run(retcode=(0, 1))
            if rc == 0:
                return True

            dcmd = diff[lfile, rfile]  | grep['-vE', grep_filter]
            dres = dcmd(retcode=(0, 1))
            if len(dres) > 10000:
                # fast track to fail? maybe should be configurable..
                # TODO Meh
                return False
            rem = dres.splitlines()
            # clean up diff crap like
            # 756587a756588,762590
            rem = [l for l in rem if not re.fullmatch(r'\d+a\d+(,\d+)?', l)]
            # TODO maybe log verbose differences to a file?
            return len(rem) == 0


    def isfsubset(left: Sequence[CRes], right: Sequence[CRes]) -> bool:
        if config.multiway:
            return _isfsubset(left, right)
        else:
            # TODO ugh. total crap
            for s1, s2 in zip(left, left[1:]):
                if not _isfsubset([s1], [s2]):
                    return False
            return True

    def lunique(l: List[CRes]) -> List[CRes]:
        return list(more_itertools.unique_everseen(l))

    left  = 0
    while left < len(cleaned):
        # TODO shit... error handling is harder...

        items  = [cleaned[left]]
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
            pivots = lunique([cleaned[lpivot], cleaned[rpivot]])

            def group() -> Group:
                # TODO oof. need to map cleaned back to original...
                return Group(
                    items =[cleaned2orig[i] for i in items],
                    pivots=[cleaned2orig[i] for i in pivots],
                )

            if right == len(cleaned):
                # end of sequence, so the whole tail is in the same group
                yield group()
                left = len(cleaned)
                break
            else:
                # try to advance right while maintaining invariants
                nitems  = lunique(items + [cleaned[right - 1], cleaned[right]])
                npivots = lunique([cleaned[lpivot], cleaned[right]])
                dominated = isfsubset(nitems, npivots)

                if not dominated:
                    # yield the last good result
                    yield group()
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
    stack.close()  # ugh. horrible
    return

    # old = cur[0]
    # old_input, old_res = old
    # if not isinstance(old_res, Exception):
    #     # meh. unlink is a bit manual, but bounds the filesystem use by two dumps
    #     # handle 'identity' cleanup -- shouldn't try to remove user files
    #     if old_res != old_input:
    #         # meh... jus in case
    #         assert str(old_res).startswith(gettempdir()), old_res
    #         old_res.unlink()  # todo no need to unlink in debug mode

    # TODO later, migrate core to use it?
    # diffing/relation generation can be generic
    #
    # TODO outputs should go one by one... zipping should be separate perhaps?
    # also we might want to retain intermediate... ugh. mindfield


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
    config = Config(
        delete_dominated=False,
        multiway=False,
    )

    p1 = tmp_path / 'p1'
    p2 = tmp_path / 'p2'
    p3 = tmp_path / 'p3'
    p4 = tmp_path / 'p4'

    p1.write_text('A\n')
    p2.write_text('B\n')
    p3.write_text('C\n')
    p4.write_text('C\nD\n')


    for gg in [
            [p1],
            [p1, p2],
            [p1, p2, p3],
            [p1, p2, p3, p4],
    ]:
        groups = list(compute_groups(
            gg,
            cleanup=_noop, max_workers=0, config=config, grep_filter=GREP_FILTER,
        ))
        instructions = groups_to_instructions(groups, config=config)
        assert [type(i) for i in instructions] == [Keep for _ in gg]


def test_filter(tmp_path: Path) -> None:
    config = Config(
        delete_dominated=False,
        multiway=False,
    )

    @contextmanager
    def remove_all_except_a(path: Path, *, wdir: Path) -> Iterator[Path]:
        clean = wdir / (path.name + '-clean')
        with path.open('r') as fo, clean.open('w') as co:
            for line in fo:
                if line == 'A\n':
                    co.write(line)
        yield clean

    p1 = tmp_path / 'p1'
    p2 = tmp_path / 'p2'
    p3 = tmp_path / 'p3'
    p4 = tmp_path / 'p4'
    paths = [p1, p2, p3, p4]

    ## these files are same as long as the filter concerned
    p1.write_text('b\nA\nc\n')
    p2.write_text('A\nx\nA\nu\n')
    p3.write_text('A\nd\n')
    p4.write_text('x\ny\n')


    groups = list(compute_groups(paths, cleanup=remove_all_except_a, max_workers=0, config=config, grep_filter=GREP_FILTER))
    instructions = groups_to_instructions(groups, config=config)
    assert [type(i) for i in instructions] == [
        Keep,
        Delete,  # should delete because after filtering only A there is no diference in files
        Keep,
        Keep
    ]


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
