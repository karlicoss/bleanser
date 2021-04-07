# TODO later, migrate core to use it?
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack
from pathlib import Path
from pprint import pprint
import re
import shutil
from subprocess import DEVNULL
from tempfile import TemporaryDirectory, gettempdir, NamedTemporaryFile
from typing import Dict, Any, Iterator, Sequence, Optional, Tuple, Optional, Union, Callable, ContextManager, Protocol, List, Set


from .common import CmpResult, Diff, Relation, Group, logger, groups_to_instructions, Keep, Delete, Config, parametrize
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
sort = local['sort']



# TODO shit. it has to own tmp dir...
# we do need a temporary copy after all?
class FileSet:
    def __init__(self, items: Sequence[Path]=(), *, wdir: Path) -> None:
        self.wdir = wdir
        self.items: List[Path] = []
        tfile = NamedTemporaryFile(dir=wdir, delete=False)
        self.merged: Path = Path(tfile.name)
        self.merged.write_text('') # meh
        self._union(*items)

    def _copy(self) -> 'FileSet':
        fs = FileSet(wdir=self.wdir)
        fs.items = list(self.items)
        shutil.copy(str(self.merged), str(fs.merged))
        return fs

    def union(self, *paths: Path) -> 'FileSet':
        u = self._copy()
        u._union(*paths)
        return u

    def _union(self, *paths: Path) -> None:
        extra = [p for p in paths if p not in self.items]
        extra = list(more_itertools.unique_everseen(extra))

        if len(extra) == 0:
            # short circuit
            return

        # todo so we could also sort individual dumps... then could use sort -m to just merge...
        # not sure what's better?
        #
        # note:
        # safe to reuse the same file as input & output
        # 'This file can be the same as one of the input files.'
        # https://pubs.opengroup.org/onlinepubs/9699919799/utilities/sort.html
        (sort['--unique'])(self.merged, *extra, '-o', self.merged)

        self.items.extend(extra)

    def issubset(self, other: 'FileSet', *, diff_filter: str) -> bool:
        lfile = self.merged
        rfile = other.merged

        # TODO tbh shoudl just use cmp/comm for the rest... considering it's all sorted
        # first check if they are identical (should be super fast, stops at the first byte difference)
        # TODO this is more or less usefless ATM.. because files in fileset are always differnet
        (rc, _, _) = cmp_cmd['--silent', lfile, rfile].run(retcode=(0, 1))
        if rc == 0:
            return True

        # if it's empty gonna strip away everything... too unsafe
        assert diff_filter.strip() != '', diff_filter

        dcmd = diff[lfile, rfile]  | grep['-vE', diff_filter]
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
        # TODO could return diff...

    def __repr__(self) -> str:
        return repr((self.items, self.merged))

    def cleanup(self) -> None:
        self.merged.unlink(missing_ok=True)


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

    f1 = lines([])
    fs_ = FS(f1)
    f2 = lines([])
    assert FS(f1).issubset(FS(f2), diff_filter=GREP_FILTER)

    fac = lines(['a', 'c'])
    fsac = FS(fac)
    assert     fs_ .issubset(fsac, diff_filter=GREP_FILTER)
    assert not fsac.issubset(fs_ , diff_filter=GREP_FILTER)
    assert     fsac.issubset(fs_ , diff_filter='.*')

    fc = lines(['c'])
    fe = lines(['e'])
    fsce = FS(fc, fe)
    assert not fsce.issubset(fsac, diff_filter=GREP_FILTER)
    assert not fsac.issubset(fsce, diff_filter=GREP_FILTER)


    fa = lines(['a'])
    fscea = fsce.union(fa)
    assert fsce.issubset(fscea, diff_filter=GREP_FILTER)


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

    cleaned2orig = {}
    cleaned = []

    # TODO eh. messy.
    stack = ExitStack()
    if _wdir is None:
        wdir = Path(stack.enter_context(TemporaryDirectory()))
    else:
        wdir = _wdir

    IRes = Union[Exception, Cleaned]
    def iter_results() -> Iterator[IRes]:
        with stack:
            for p in paths:
                res: IRes
                try:
                    res = stack.enter_context(cleanup(p, wdir=wdir))
                except Exception as e:
                    logger.exception(e)
                    res = e
                cleaned2orig[res] = p
                cleaned.append(res)
                yield res


    def fset(*paths: Path) -> FileSet:
        return FileSet(paths, wdir=wdir)

    # OMG. extremely hacky...
    ires = more_itertools.peekable(iter_results())

    total = len(paths)

    def unlink_tmp_output(cleaned: Path) -> None:
        # meh. unlink is a bit manual, but bounds the filesystem use by two dumps
        orig = cleaned2orig[cleaned]
        if orig == cleaned:
            # handle 'identity' cleanup -- shouldn't try to remove user files
            return
        # meh... just in case
        assert str(cleaned).startswith(gettempdir()), cleaned
        if cleaned.exists(): # todo maybe get rid of this warning...
            logger.warning('unlinking: %s', cleaned)
        # todo no need to unlink in debug mode
        cleaned.unlink(missing_ok=True)

    left  = 0
    # empty fileset is easier than optional
    items = fset()
    while left < total:
        lfile = ires[left]

        items.cleanup()

        if isinstance(lfile, Exception):
            yield Group(
                items =[cleaned2orig[lfile]],
                pivots=[cleaned2orig[lfile]],
            )
            left += 1
            continue

        items = fset(lfile)

        lpivot = left
        lpfile = lfile

        rpivot = left
        rpfile = lfile
        # invaraint
        # - items, lpivot, rpivot are all valid
        # - sets corresponding to lpivot + rpivot contain all of 'items'
        # next we attempt to
        # - rpivot: hopefully advance as much as possible
        # - items : expand to include as much as possible

        # eh. empty fileset is easier than optional
        pivots = fset()
        right = left + 1
        while True:
            pivots.cleanup()
            pivots = fset(lpfile, rpfile)

            def group(rm_last: bool) -> Group:
                gitems = items.items
                g =  Group(
                    items =[cleaned2orig[i] for i in gitems],
                    pivots=[cleaned2orig[i] for i in pivots.items],
                )
                to_unlink = gitems[: len(gitems) if rm_last else -1]
                for i in to_unlink:
                    unlink_tmp_output(i)
                return g

            if right == total:
                # end of sequence, so the whole tail is in the same group
                left = total
                yield group(rm_last=True)
                break

            # else try to advance right while maintaining invariants
            right_res = ires[right]

            next_state: Optional[Tuple[FileSet, Path]]
            if isinstance(right_res, Exception):
                # short circuit... error itself will be handled when right_res is the leftmost element
                next_state = None
            else:
                nitems  = items.union(right_res)

                if config.multiway:
                    # in multiway mode we check if the boundaries (pivots) contain the rest
                    npivots = fset(lpfile, right_res)
                    dominated = nitems.issubset(npivots, diff_filter=grep_filter)
                    npivots.cleanup()
                else:
                    # in two-way mode we check if successive paths include each other
                    before_right = nitems.items[-2]
                    # TODO ugh. crappy sets... really need to make lazier somehow...
                    s1 = fset(before_right)
                    s2 = fset(right_res)
                    dominated = s1.issubset(s2, diff_filter=grep_filter)
                    s1.cleanup()
                    s2.cleanup()

                if dominated:
                    next_state = (nitems, right_res)
                else:
                    next_state = None
                    nitems.cleanup()

            if next_state is None:
                # ugh. a bit crap, but seems that a special case is necessary
                # otherwise left won't ever get advanced?
                if len(pivots.items) == 2:
                    left = rpivot
                    rm_last = False
                else:
                    left = rpivot + 1
                    rm_last = True
                yield group(rm_last=rm_last)
                break

            # else advance it, keeping lpivot unchanged
            (nitems, rres) = next_state
            items.cleanup()
            items = nitems
            rpivot = right
            rpfile = rres
            # right will not be read anymore?

            # intermediate files won't be used anymore
            for i in items.items[1: -1]:
                unlink_tmp_output(i)

            right += 1
        pivots.cleanup()

    # FIXME use ctx managers for this stuff... somehow
    pivots.cleanup()
    items.cleanup()

    # meh. hacky but sort of does the trick
    cached = len(getattr(ires, '_cache'))
    assert cached == total, 'Iterator should be fully processed!'


# note: also some tests in sqlite.py


@parametrize('multiway', [True, False])
def test_bounded_resources(multiway: bool, tmp_path: Path) -> None:
    """
    Check that relation processing is iterative in terms of not using too much disk space for temporary files
    """

    # max size of each file
    one_mb = 1_000_000
    text = 'x' * one_mb + '\n'


    idir = tmp_path / 'idir'
    wdir = tmp_path / 'wdir'
    idir.mkdir()
    wdir.mkdir()

    # each file would be approx 1mb in size
    inputs = []
    for g in range(4): # 4 groups
        for i in range(20): # 20 backups in each group
            ip = idir / f'{g}_{i}.txt'
            text += str(i) + '\n'
            ip.write_text(text)
            inputs.append(ip)
        ip = idir / f'{g}_sep.txt'
        ip.write_text('GARBAGE')
        inputs.append(ip)
    ##

    idx = 0
    def check_wdir_space() -> None:
        nonlocal idx
        from .utils import total_dir_size
        logger.warning('ITERATION: %s', idx)
        ds = total_dir_size(wdir)

        # 7 is a bit much... but currently it is what it is, can be tighter later
        # basically
        # - at every point we keep both pivots (2 x 1mb)
        # - we keep the merged bit (about 1mb in this specific test cause of overlap)
        # - we keep one next file (1mb)
        # - we might need to copy the merged bit at some point as well to test it as a candidate for next
        threshold = 7 * one_mb
        # from subprocess import check_call
        # check_call(['ls', '-al', wdir])
        assert ds < threshold, ds
        # TODO!
        # assert r.diff.cmp == CmpResult.DOMINATES

        if idx > 3:
            # in 'steady' mode should take some space? more of a sanity check..
            assert ds > one_mb, ds
        idx += 1


    @contextmanager
    def dummy(path: Path, *, wdir: Path) -> Iterator[Path]:
        tp = wdir / (path.name + '-dump')
        tp.write_text(path.read_text())
        # ugh. it's the only place we can hook in to do frequent checks..
        check_wdir_space()
        yield tp

    config = Config(multiway=multiway)
    func = lambda paths: compute_groups(paths, cleanup=dummy, max_workers=0, grep_filter=GREP_FILTER, config=config, _wdir=wdir)

    # force it to compute
    groups = list(func(inputs))
    # if all good, should remove all the intermediate ones?
    # so
    # - in twoway   mode: 4 seps + 2 boundary files in each group = 12 groups
    # - in multiway mode: seps end up as part of groups, so it's just 8 groups
    # if it goes bad, there will be error on each step
    expected = 8 if multiway else 12
    assert len(groups) == expected


@contextmanager
def _noop(path: Path, *, wdir: Path) -> Iterator[Path]:
    yield path


@parametrize('multiway', [False, True])
def test_simple(multiway: bool, tmp_path: Path) -> None:
    config = Config(
        delete_dominated=False,
        multiway=multiway,
    )

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

