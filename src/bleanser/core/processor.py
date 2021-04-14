# TODO later, migrate core to use it?
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, ExitStack, closing
import os
from pathlib import Path
from pprint import pprint
import re
import shutil
from subprocess import DEVNULL, check_call
from tempfile import TemporaryDirectory, gettempdir, NamedTemporaryFile
from time import time
from typing import Dict, Any, Iterator, Sequence, Optional, Tuple, Optional, Union, Callable, ContextManager, Protocol, List, Set, ClassVar, Type, Iterable


from .common import Diff, Relation, Group, logger, Config, parametrize
from .common import Instruction, Keep, Delete
from .utils import DummyExecutor, total_dir_size


import more_itertools
from plumbum import local # type: ignore



class BaseNormaliser:
    # FIXME make default = None?
    DIFF_FILTER: ClassVar[Optional[str]]

    ## user overridable configs
    DELETE_DOMINATED: ClassVar[bool] = False
    MULTIWAY: ClassVar[bool] = False
    ##

    # TODO ugh. annoying that path is duplicated in two places...
    def __init__(self, path: Path) -> None:
        pass

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        raise NotImplementedError

    @classmethod
    def main(cls) -> None:
        from .main import main as run_main
        run_main(Normaliser=cls)


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
        diff_filter: Optional[str],
        config: Config,
        _wdir: Optional[Path]=None,
) -> Iterator[Group]:
    assert len(paths) == len(set(paths)), paths  # just in case
    assert len(paths) > 0 # just in case

    # if wdir is passed will use this dir instead of a temporary
    # messy but makes debugging a bit easier..
    pool = DummyExecutor() if max_workers == 0 else ThreadPoolExecutor(max_workers=max_workers)
    with pool:
        workers = getattr(pool, '_max_workers')
        workers = min(workers, len(paths))  # no point in using too many workers
        logger.info('using %d workers', workers)

        chunks = []
        futures = []
        for paths_chunk in more_itertools.divide(workers, paths):
            pp = list(paths_chunk)
            if len(pp) == 0:
                continue
            chunks.append(pp)
            # force iterator if we're using more than one thread
            # otherwise it'll still be basically serial execution
            mforce = list if workers > 1 else lambda x: x
            futures.append(pool.submit(
                lambda *args, **kwargs: mforce(_compute_groups_serial(*args, **kwargs)),
                paths=pp,
                cleanup=cleanup,
                diff_filter=diff_filter,
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


def do_diff(lfile: Path, rfile: Path, *, diff_filter: Optional[str]) -> List[str]:
    dcmd = diff[lfile, rfile]
    if diff_filter is not None:
        # if it's empty gonna strip away everything... too unsafe
        assert diff_filter.strip() != '', diff_filter
        dcmd = dcmd | grep['-vE', diff_filter]
    diff_lines = dcmd(retcode=(0, 1))
    # FIXME not sure about it...
    # if len(dres) > 10000:
    #     # fast track to fail? maybe should be configurable..
    #     # TODO Meh
    #     return False
    rem = diff_lines.splitlines()
    # clean up diff crap like
    # 756587a756588,762590
    rem = [l for l in rem if not re.fullmatch(r'\d+a\d+(,\d+)?', l)]

    # TODO not sure what's the best way to provide some quick debug means...
    # need grep -C or something like that...
    if len(rem) > 0:
        logger.debug(f'diff %s %s', lfile, rfile)
        logger.debug('vvvvvvvvvvvvv DIFF vvvvvvvvvvvvv')
        for line in rem:
            logger.debug(line)
        logger.debug('^^^^^^^^^^^^^ DIFF ^^^^^^^^^^^^^')
    return rem


# TODO shit. it has to own tmp dir...
# we do need a temporary copy after all?
class FileSet:
    def __init__(self, items: Sequence[Path]=(), *, wdir: Path) -> None:
        self.wdir = wdir
        self.items: List[Path] = []
        tfile = NamedTemporaryFile(dir=self.wdir, delete=False)
        self.merged = Path(tfile.name)
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

        # todo so we could also sort individual dumps... then could use sort --merged to just merge...
        # it seems to be marginally better, like 25% maybe
        # makes it a bit more compliacted on
        # if I do implement it:
        # - add '--merge' flag below
        # - add sort --unique in sqlite.py
        # - add sort --check after we got cleaned file
        #   NOTE: can't make it in-place either because might modify the input file in case of 'idenity' cleaner

        # note:
        # safe to reuse the same file as input & output
        # 'This file can be the same as one of the input files.'
        # https://pubs.opengroup.org/onlinepubs/9699919799/utilities/sort.html

        # allow it not to have merged file if set is empty
        tomerge = ([] if len(self.items) == 0 else [self.merged]) + extra

        # sort also has --parallel option... but pretty pointless, in most cases we'll be merging two files?
        (sort['--unique'])(*tomerge, '-o', self.merged)

        self.items.extend(extra)

    def issubset(self, other: 'FileSet', *, diff_filter: str) -> bool:
        # short circuit
        # this doesn't really speed up much though? so guess better to keep the code more uniform..
        # if set(self.items) <= set(other.items):
        #     return True
        lfile = self.merged
        rfile = other.merged
        # upd: hmm, this function is actually super fast... guess diff is quite a bit optimized

        # TODO tbh shoudl just use cmp/comm for the rest... considering it's all sorted
        # first check if they are identical (should be super fast, stops at the first byte difference)
        # TODO this is more or less usefless ATM.. because files in fileset are always differnet
        (rc, _, _) = cmp_cmd['--silent', lfile, rfile].run(retcode=(0, 1))
        if rc == 0:
            return True

        remaining = do_diff(lfile, rfile, diff_filter=diff_filter)
        # TODO maybe log verbose differences to a file?
        return len(remaining) == 0
        # TODO could return diff...

    def __repr__(self) -> str:
        return repr((self.items, self.merged))

    def __enter__(self) -> 'FileSet':
        return self

    def __exit__(self, type, value, tb) -> None:
        self.close()

    def close(self) -> None:
        self.merged.unlink(missing_ok=True)


_FILTER_ALL_ADDED = '> '


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

    fac = lines(['a', 'c'])
    fsac = FS(fac)
    assert     fs_ .issubset(fsac, diff_filter=dfilter)
    assert not fsac.issubset(fs_ , diff_filter=dfilter)
    assert     fsac.issubset(fs_ , diff_filter='.*')

    fc = lines(['c'])
    fe = lines(['e'])
    fsce = FS(fc, fe)
    assert not fsce.issubset(fsac, diff_filter=_FILTER_ALL_ADDED)
    assert not fsac.issubset(fsce, diff_filter=_FILTER_ALL_ADDED)


    fa = lines(['a'])
    fscea = fsce.union(fa)
    assert fsce.issubset(fscea, diff_filter=_FILTER_ALL_ADDED)


# todo these are already normalized paths?
# although then harder to handle exceptions... ugh
def _compute_groups_serial(
        paths: Sequence[Path],
        *,
        cleanup: Cleaner,
        diff_filter: str,
        config: Config,
        _wdir: Optional[Path],
) -> Iterator[Group]:
    assert len(paths) > 0

    cleaned2orig = {}
    cleaned = []

    wdir: Path

    IRes = Union[Exception, Cleaned]
    def iter_results() -> Iterator[IRes]:
        with ExitStack() as istack:
            # ugh. what a mess
            nonlocal wdir
            if _wdir is None:
                wdir = Path(istack.enter_context(TemporaryDirectory()))
            else:
                wdir = _wdir
            for p in paths:
                res: IRes
                ds = total_dir_size(wdir)
                logger.debug('total wdir(%s) size: %s', wdir, ds)
                before = time()
                # pass it a unique dir so they don't mess up each other
                pwdir = Path(istack.enter_context(TemporaryDirectory(dir=wdir)))
                try:
                    res = istack.enter_context(cleanup(p, wdir=pwdir))
                except Exception as e:
                    logger.exception(e)
                    res = e
                after = time()
                logger.debug('cleanup(%s): took %.1f seconds', p, after - before)
                cleaned2orig[res] = p
                cleaned.append(res)
                yield res


    def fset(*paths: Path) -> FileSet:
        return FileSet(paths, wdir=wdir)

    def unlink_tmp_output(cleaned: Path) -> None:
        # meh. unlink is a bit manual, but bounds the filesystem use by two dumps
        orig = cleaned2orig[cleaned]
        if orig == cleaned:
            # handle 'identity' cleanup -- shouldn't try to remove user files
            return
        # meh... just in case
        assert str(cleaned).startswith(gettempdir()), cleaned
        if cleaned.exists(): # todo maybe get rid of this warning...
            logger.debug('unlinking: %s', cleaned)
        # todo no need to unlink in debug mode
        cleaned.unlink(missing_ok=True)

    total = len(paths)

    # ok. this is a bit hacky
    # ... but making it properly iterative would be complicated and error prone
    # since sometimes we do need lookahead (for right + 1)
    # so using peekable seems like a good compromise
    ires = more_itertools.peekable(iter_results())
    # it would be nice to also release older iterator entries (calling next())
    # but it seems to change indexing... so a bit of a mess.

    ires[0] # ugh. a bit crap, but we're nudging it to initialize wdir...


    left  = 0
    # empty fileset is easier than optional
    items = fset()
    while left < total:
        lfile = ires[left]

        if isinstance(lfile, Exception):
            yield Group(
                items =[cleaned2orig[lfile]],
                pivots=[cleaned2orig[lfile]],
            )
            left += 1
            continue

        items.close()
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

        right = left + 1
        while True:
            with ExitStack() as rstack:
                pivots = rstack.enter_context(fset(lpfile, rpfile))

                def group(rm_last: bool) -> Group:
                    gitems = items.items
                    citems = [cleaned2orig[i] for i in gitems]
                    cpivots = [cleaned2orig[i] for i in pivots.items]
                    g =  Group(
                        items =citems,
                        pivots=cpivots,
                    )
                    logger.debug('emitting group pivoted on %s, size %d', list(map(str, cpivots)), len(citems))
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
                        npivots = rstack.enter_context(fset(lpfile, right_res))
                        dominated = nitems.issubset(npivots, diff_filter=diff_filter)
                    else:
                        # in two-way mode we check if successive paths include each other
                        before_right = nitems.items[-2]
                        s1 = rstack.enter_context(fset(before_right))
                        s2 = rstack.enter_context(fset(right_res))
                        dominated = s1.issubset(s2, diff_filter=diff_filter)

                    if dominated:
                        next_state = (nitems, right_res)
                    else:
                        next_state = None
                        rstack.push(nitems)  # won't need it anymore, recycle

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
                rstack.push(items)  # recycle
                items = nitems
                rpivot = right
                rpfile = rres
                # right will not be read anymore?

                # intermediate files won't be used anymore
                for i in items.items[1: -1]:
                    unlink_tmp_output(i)

                right += 1

    items.close()

    # meh. hacky but sort of does the trick
    cached = len(getattr(ires, '_cache'))
    assert cached == total, 'Iterator should be fully processed!'


# note: also some tests in sqlite.py


@parametrize('multiway,randomize', [
    (False, False),
    (True , False),
    (True , True ),
    (False, True ),
])
def test_bounded_resources(multiway: bool, randomize: bool, tmp_path: Path) -> None:
    """
    Check that relation processing is iterative in terms of not using too much disk space for temporary files
    """

    # max size of each file
    one_mb = 1_000_000
    text = 'x' * one_mb + '\n'


    idir = tmp_path / 'idir'
    gwdir = tmp_path / 'wdir'  # 'global' wdir
    idir.mkdir()
    gwdir.mkdir()

    from random import Random
    import string
    r = Random(0)
    # each file would be approx 1mb in size
    inputs = []
    for g in range(4): # 4 groups
        for i in range(20): # 20 backups in each group
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
    wdir_spaces = []
    def check_wdir_space() -> None:
        nonlocal idx
        # logger.warning('ITERATION: %s', idx)
        ds = total_dir_size(gwdir)

        # 7 is a bit much... but currently it is what it is, can be tighter later
        # basically
        # - at every point we keep both pivots (2 x 1mb)
        # - we keep the merged bit (about 1mb in this specific test cause of overlap)
        # - we keep one next file (1mb)
        # - we might need to copy the merged bit at some point as well to test it as a candidate for next
        threshold = 7 * one_mb
        # check_call(['ls', '-al', gwdir])

        # raise baseexception, so it propagates all the way up and doesn't trigget defensive logic
        if ds > threshold:
            raise BaseException("working dir takes too much space")

        wdir_spaces.append(ds)
        idx += 1


    @contextmanager
    def dummy(path: Path, *, wdir: Path) -> Iterator[Path]:
        tp = wdir / (path.name + '-dump')
        tp.write_text(path.read_text())
        # ugh. it's the only place we can hook in to do frequent checks..
        check_wdir_space()
        yield tp

    config = Config(multiway=multiway)
    func = lambda paths: compute_groups(paths, cleanup=dummy, max_workers=0, diff_filter=_FILTER_ALL_ADDED, config=config, _wdir=gwdir)

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
    took_space = len([x for x in wdir_spaces if x > one_mb])
    assert took_space > 20


@parametrize('multiway', [False, True])
def test_many_files(multiway: bool, tmp_path: Path) -> None:
    config = Config(multiway=multiway)
    N = 2000

    @contextmanager
    def dummy(path: Path, *, wdir: Path) -> Iterator[Path]:
        tp = wdir / (path.name + '-dump')
        tp.write_text(path.read_text())
        yield tp

    paths = []
    for i in range(N):
        p = tmp_path / f'{i:05}'
        paths.append(p)
        p.write_text(str(i % 10 > 5) + '\n')

    groups = []
    for group in compute_groups(paths, cleanup=dummy, max_workers=0, diff_filter=_FILTER_ALL_ADDED, config=config):
        groups.append(group)
    # shouldn't crash due to open files or something, at least
    expected = 399 if multiway else 799
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
            cleanup=_noop, max_workers=0, config=config, diff_filter=_FILTER_ALL_ADDED,
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


    groups = list(compute_groups(paths, cleanup=remove_all_except_a, max_workers=0, config=config, diff_filter=_FILTER_ALL_ADDED))
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
    groups = list(compute_groups(paths, cleanup=_noop, max_workers=0, config=config, diff_filter=_FILTER_ALL_ADDED))
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
    groups = list(compute_groups(paths, cleanup=_noop, max_workers=0, diff_filter=_FILTER_ALL_ADDED, config=config))
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


# TODO config is unused here?? not sure
def groups_to_instructions(groups: Iterable[Group], *, config: Config) -> Iterator[Instruction]:
    done: Dict[Path, Instruction] = {}

    for group in groups:
        # TODO groups can overlap on their pivots.. but nothing else

        # TODO add split method??
        for i in group.items:
            if i in group.pivots:
                # pivots might be already emitted py the previous groups
                pi = done.get(i)
                if pi is None:
                    keep = Keep(path=i, group=group)
                    yield keep
                    done[i] = keep
                else:
                    if not isinstance(pi, Keep):
                        raise RuntimeError(f'{i}: used both as pivot and non-pivot: {group} AND {pi}')
            else:
                if i in done:
                    raise RuntimeError(f'{i}: occurs in multiple groups: {group} AND {done[i]}')
                assert i not in done, (i, done)
                deli = Delete(path=i, group=group)
                yield deli
                done[i] = deli


def test_groups_to_instructions() -> None:
    def do(*pp: Sequence[str], config=Config()):
        ppp = [list(map(Path, s)) for s in pp]
        # for this test we assume pivots are just at the edges
        grit = (
            Group(
                items=p,
                pivots=(p[0], p[-1]),
            ) for p in ppp
        )
        res = groups_to_instructions(list(grit), config=config)
        return [(str(p.path), {Keep: 'keep', Delete: 'delete'}[type(p)]) for p in res]

    assert do(
        ('a', 'b'),
    ) == [
        ('a', 'keep'),
        ('b', 'keep'),
    ]

    assert do(
        ('0', 'a'          ),
        ('a', 'b', 'c', 'd'),
    ) == [
        ('0', 'keep'  ),
        ('a', 'keep'  ),
        ('b', 'delete'),
        ('c', 'delete'),
        ('d', 'keep'  ),
    ]


    # TODO shit. how to test this now?
    # maybe it's the config -- delete both pivots or not? not sure
   #inputs = [
   #    ('a', 'b', CR.SAME     ),
   #    ('b', 'c', CR.DIFFERENT),
   #    ('c', 'd', CR.DOMINATES),
   #    ('d', 'e', CR.SAME     ),
   #    ('e', 'f', CR.DOMINATES),
   #    ('f', 'g', CR.DIFFERENT),
   #    ('g', 'h', CR.SAME     ),
   #]
   #
   #assert do(*inputs) == [
   #    ('a', 'keep'  ),
   #    ('b', 'keep'  ),
   #    ('c', 'keep'  ),
   #    ('d', 'keep'  ),
   #    ('e', 'keep'  ),
   #    ('f', 'keep'  ),
   #    ('g', 'keep'  ),
   #    ('h', 'keep'  ),
   #]
   #
   #assert do(*inputs, config=Config(delete_dominated=True)) == [
   #    ('a', 'keep'  ),
   #    ('b', 'keep'  ),
   #    ('c', 'keep'  ),
   #    ('d', 'delete'),
   #    ('e', 'delete'),
   #    ('f', 'keep'  ),
   #    ('g', 'keep'  ),
   #    ('h', 'keep'  ),
   #]

    import pytest  # type: ignore

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


def compute_instructions(
        paths: Sequence[Path],
        *,
        Normaliser: Type[BaseNormaliser],
        max_workers: Optional[int],
) -> Iterator[Instruction]:
    def cleanup(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = Normaliser(path)
        return n.do_cleanup(path, wdir=wdir)

    cfg = Config(
        delete_dominated=Normaliser.DELETE_DOMINATED,
        multiway=Normaliser.MULTIWAY,
    )
    groups: Iterable[Group] = compute_groups(
        paths=paths,
        cleanup=cleanup,
        diff_filter=Normaliser.DIFF_FILTER,
        config=cfg,
        max_workers=max_workers,
    )
    instructions: Iterable[Instruction] = groups_to_instructions(groups, config=cfg)
    total = len(paths)
    # TODO eh. could at least dump dry mode stats here...
    done = 0
    for i, ins in enumerate(instructions):
        logger.debug(f'{i:<3}/{total:<3} %s: %s', ins.path, type(ins))
        yield ins
        done += 1
    assert done == len(paths)  # just in case


# FIXME add a test

from .common import Mode, Dry, Move, Remove
def apply_instructions(instructions: Iterable[Instruction], *, mode: Mode=Dry()) -> None:
    import click  # type: ignore

    totals: str
    if not isinstance(mode, Dry):
        # force for safety
        instructions = list(instructions)
        totals = f'{len(instructions):>3}'
    else:
        totals = '???'

    rm_action = {
        Dry   : 'REMOVE (dry mode)',
        Move  : 'MOVE             ',
        Remove: 'REMOVE           ',
    }[type(mode)]

    tot_files = 0
    rem_files = 0
    tot_bytes = 0
    rem_bytes = 0

    def stat() -> str:
        tmb = tot_bytes / 2 ** 20
        rmb = rem_bytes / 2 ** 20
        return f'cleaned so far: {int(rmb):>4} Mb /{int(tmb):>4} Mb , {rem_files:>3} /{tot_files:>3} files'

    to_delete = []
    for idx, ins in enumerate(instructions):
        ip = ins.path
        sz = ip.stat().st_size
        tot_bytes += sz
        tot_files += 1
        action: str
        if   isinstance(ins, Keep):
            action = 'will keep        '
        elif isinstance(ins, Delete):
            action = rm_action
            rem_bytes += sz
            rem_files += 1
            to_delete.append(ins.path)
        else:
            raise RuntimeError(ins)
        logger.info(f'processing {idx:>4}/{totals:>4} %s : %s  ; %s', ip, action, stat())

    logger.info('SUMMARY: %s', stat())

    if isinstance(mode, Dry):
        logger.info('dry mode! not touching anything')
        return

    if len(to_delete) == 0:
        logger.info('no files to cleanup!')
        return

    if not click.confirm(f'Ready to {rm_action.strip().lower()} {len(to_delete)} files?', abort=True):
        return

    move_to: Optional[Path] = None
    if   isinstance(mode, Move):
        move_to = mode.path
        # just in case
        assert move_to.is_absolute(), move_to
    elif isinstance(mode, Remove):
        pass
    else:
        raise RuntimeError(mode, type(mode))

    import shutil
    for p in to_delete:
        assert p.is_absolute(), p  # just in case
        if move_to is not None:
            tgt = move_to / Path(*p.parts[1:])
            tgt.parent.mkdir(parents=True, exist_ok=True)
            logger.info('mv %s %s', p, tgt)
            shutil.move(str(p), str(tgt))
        else:
            logger.info('rm %s', p)
            p.unlink()


def compute_diff(path1: Path, path2: Path, *, Normaliser) -> List[str]:
    # meh. copy pasted...
    def cleanup(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = Normaliser(path)  # type: ignore  # meh
        return n.do_cleanup(path, wdir=wdir)

    from .processor import do_diff
    with TemporaryDirectory() as td1, TemporaryDirectory() as td2:
        with cleanup(path1, wdir=Path(td1)) as res1, cleanup(path2, wdir=Path(td2)) as res2:
            # ok, I guess diff_filter=None makes more sense here?
            # would mean it shows the whole thing
            # meh
            if os.environ.get('USE_VIMDIFF', None) == 'yes':
                os.execlp('vimdiff', 'vimdiff', str(res1), str(res2))
            return do_diff(res1, res2, diff_filter=None)
