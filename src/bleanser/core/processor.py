# TODO later, migrate core to use it?
from contextlib import contextmanager, ExitStack
import os
from pathlib import Path
import re
import shutil
import sys
from subprocess import check_call
from tempfile import TemporaryDirectory, gettempdir, NamedTemporaryFile
from time import time
from typing import Dict, Iterator, Sequence, Optional, Tuple, Optional, Union, ContextManager, Protocol, List, Set, ClassVar, Type, Iterable, NoReturn, Any, Callable


from .common import Group, logger, Config, parametrize
from .common import Instruction, Keep, Prune
from .common import divide_by_size
from .utils import total_dir_size
from .ext.dummy_executor import DummyExecutor


from kompress import CPath
import more_itertools
from plumbum import local # type: ignore

# helper functions for normalisers
def unique_file_in_tempdir(*, input_filepath: Path, wdir: Path, suffix: Optional[str] = None) -> Path:
    '''
    this doesn't actually create the temp dir, wdir is already made/cleaned up somewhere above

    say for a file like /home/user/data/something/else.json
    this creates a file like:

    /tmpdir/home/user/data/something/else-cleaned

    If suffix is not provided, it is NOT assumed/added automatically
    '''
    # make sure these are absolute
    input_filepath = input_filepath.absolute().resolve()
    assert wdir.is_absolute()

    # is useful to keep the suffix the same, or let the user customize
    # in case some library code elsewhere a user might
    # use to process this uses the filetype to detect the filetype
    suffix = suffix or ''
    cleaned_dir = wdir / Path(*input_filepath.parts[1:])
    cleaned_path = cleaned_dir / (input_filepath.stem + '-cleaned' + suffix)

    # if this already exists somehow, use tempfile to add some random noise to the filename
    cleaned_path.parent.mkdir(parents=True, exist_ok=True)
    return cleaned_path

# meh... see Fileset._union
# this gives it a bit of a speedup when comparing
def sort_file(filepath: Union[str, Path]) -> None:
    check_call(['sort', '-o', str(filepath), str(filepath)])



class BaseNormaliser:
    ## user overridable configs
    PRUNE_DOMINATED: ClassVar[bool] = False
    MULTIWAY       : ClassVar[bool] = False
    ##


    # todo maybe get rid of it? might be overridden by subclasses but probs. shouldn't
    _DIFF_FILTER: ClassVar[Optional[str]] = '> '

    # take in input path
    # output file descriptor (could be tmp dir based or in-memory?)
    # would be perfect time to unpack it?
    # or allow outputting either Path or fd? dunno

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        '''
        path: input filepath. could possibly be compressed
        wdir: working directory, where temporary files are written

        currently, this just returns the entire file after possible decompressing it

        subclasses would typically override this, reading from 'upath' as the input file

        '''
        with self.unpacked(path=path, wdir=wdir) as upath:
            yield upath

        # e.g., subclasses might read/parse upath, write to some unique tempfile
        # and yield the 'cleaned' path
        #
        # for an example, see modules/json_new.py


    @contextmanager
    def unpacked(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        '''
        path: input filepath. this could be compressed or not, CPath with transparently decompress
        wdir: working directory, where temporary files are written

        this takes the input file from the user, and uses CPath to decompress it if its a compressed file
        otherwise, this just reads the file as normal and writes to the temporary directory

        subclasses could override this to do some custom type of unpacking. e.g. if its a zipfile
        and you need to extract it and return a particular file as the 'cleaned_path', you can
        do that here

        Then, in do_cleanup, it uses the unpacked/extracted file from here
        '''
        # todo ok, kinda annoying that a lot of time is spent unpacking xz files...
        # not sure what to do about it
        with CPath(str(path)).open(mode='rb') as fo:
            res = fo.read()

        # TODO maybe keep track of original files in the Normaliser and assert before removing anything
        # this would ensure the logic for using extra files is safe

        # TODO not sure if cleaned path _has_ to be in wdir? can we return the orig path?
        # maybe if the cleanup method is not implemented?
        cleaned_path = unique_file_in_tempdir(input_filepath=path, wdir=wdir)
        cleaned_path.write_bytes(res)
        # writing to tmp does take a while... hmm
        yield cleaned_path

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
        threads: Optional[int]=None,
        diff_filter: Optional[str],
        config: Config,
        _wdir: Optional[Path]=None,
) -> Iterator[Group]:
    assert len(paths) == len(set(paths)), paths  # just in case
    assert len(paths) > 0 # just in case

    # if wdir is passed will use this dir instead of a temporary
    # messy but makes debugging a bit easier..
    from concurrent.futures import ProcessPoolExecutor as Pool, Future
    pool = DummyExecutor() if threads is None else Pool(max_workers=None if threads == 0 else threads)
    with pool:
        workers = getattr(pool, '_max_workers')
        workers = min(workers, len(paths))  # no point in using too many workers
        logger.info('using %d workers', workers)

        chunks = []
        futures: List[Future] = []
        for paths_chunk in divide_by_size(buckets=workers, paths=paths):
            pp = list(paths_chunk)
            if len(pp) == 0:
                continue
            chunks.append(pp)
            # force iterator if we're using more than one thread
            # otherwise it'll still be basically serial execution
            # in addition, multiprocess would fail to pickle returned iterator
            func: Callable[..., Iterable[Group]]
            # note: separate declaration and if statement makes mypy happy
            if threads is not None:
                func = _compute_groups_serial_as_list
            else:
                func = _compute_groups_serial
            futures.append(pool.submit(
                func,
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

# ok so there is no time difference if using special diff line format
# $ hyperfine -i -- 'diff --new-line-format="> %L" --old-line-format="" --unchanged-line-format="" tmp/lastfm_2017-08-29_sorted tmp/lastfm_2017-09-01_sorted'
# Benchmark 1: diff --new-line-format="> %L" --old-line-format="" --unchanged-line-format="" tmp/lastfm_2017-08-29_sorted tmp/lastfm_2017-09-01_sorted
#   Time (mean ± σ):      28.9 ms ±   2.3 ms    [User: 17.5 ms, System: 11.1 ms]
#   Range (min … max):    26.4 ms …  37.1 ms    109 runs
#
#   Warning: Ignoring non-zero exit code.
#
# $ hyperfine -i -- 'diff tmp/lastfm_2017-08-29_sorted tmp/lastfm_2017-09-01_sorted'
# Benchmark 1: diff tmp/lastfm_2017-08-29_sorted tmp/lastfm_2017-09-01_sorted
#   Time (mean ± σ):      27.6 ms ±   1.5 ms    [User: 16.8 ms, System: 10.7 ms]
#   Range (min … max):    26.0 ms …  32.9 ms    107 runs
#
#   Warning: Ignoring non-zero exit code.


def do_diff(lfile: Path, rfile: Path, *, diff_filter: Optional[str]) -> List[str]:
    dcmd = diff[lfile, rfile]
    filter_crap = True
    if diff_filter is not None:
        # if it's empty gonna strip away everything... too unsafe
        assert diff_filter.strip() != '', diff_filter

        # shortcut...
        if diff_filter == '> ':
            # TODO wtf?? is plumbum messing with "" escaping or something??
            # passing '--old-line-format="< %L"' ended up in extra double quotes emitted
            dcmd = dcmd['--new-line-format=', '--unchanged-line-format=', '--old-line-format=< %L']
            filter_crap = False
        else:
            dcmd = dcmd | grep['-vE', '^' + diff_filter]
    diff_lines = dcmd(retcode=(0, 1))

    # FIXME move splitlines under print_diff and len() check
    rem = diff_lines.splitlines()
    if filter_crap:
        # TODO remove later perhaps once we make diff_filter non-configurable
        # clean up diff crap like
        # 756587a756588,762590 and 88888,88890d88639
        rem = [l for l in rem if not re.fullmatch(r'(\d+,)?\d+[ad]\d+(,\d+)?', l)]

    # TODO maybe log in a separate file
    # TODO not sure what's the best way to provide some quick debug means...
    # need grep -C or something like that...
    print_diff = True
    if print_diff and len(rem) > 0:
        logger.debug('diff %s %s', lfile, rfile)
        logger.debug('vvvvvvvvvvvvv DIFF vvvvvvvvvvvvv')
        if sys.stdin.isatty():
            # only log when running interactively... otherwise spams syslog too much
            for line in rem:
                logger.debug(line)
        else:
            logger.debug('non-interactive session, skipping diff logging (otherwise spams syslog too much)')
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

        # hmm sadly sort command doesn't detect it itself?
        # todo make less hacky... ideally the callee would maintain the sorted files
        is_sorted = []
        for p in tomerge: # todo no need to check self.merged?
            (rc, _, _) = sort['--check', p].run(retcode=(0, 1))
            is_sorted.append(rc == 0)
        mflag = []
        if all(is_sorted):
            mflag = ['--merge']

        # sort also has --parallel option... but pretty pointless, in most cases we'll be merging two files?
        (sort['--unique'])(*mflag, *tomerge, '-o', self.merged)

        self.items.extend(extra)

    def issame(self, other: 'FileSet') -> bool:
        lfile = self.merged
        rfile = other.merged
        # TODO meh. maybe get rid of cmp, it's not really faster
        # even on exactly same file (copy) it seemed to be slower
        # https://unix.stackexchange.com/questions/153286/is-cmp-faster-than-diff-q
        (rc, _, _) = cmp_cmd['--silent', lfile, rfile].run(retcode=(0, 1))
        return rc == 0

    def issubset(self, other: 'FileSet', *, diff_filter: Optional[str]) -> bool:
        # short circuit
        # this doesn't really speed up much though? so guess better to keep the code more uniform..
        # if set(self.items) <= set(other.items):
        #     return True
        lfile = self.merged
        rfile = other.merged
        # upd: hmm, this function is actually super fast... guess diff is quite a bit optimized

        # TODO tbh should just use cmp/comm for the rest... considering it's all sorted
        # first check if they are identical (should be super fast, stops at the first byte difference)
        # TODO this is more or less usefless ATM.. because files in fileset are always different
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


# TODO reuse it in normalisers
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

    assert FS(f1).issame(FS(f2))

    fsac = FS(lines(['a', 'c']))

    assert     fsac.issame(FS(lines(['a', 'c'])))
    assert not fsac.issame(FS(lines(['a', 'c', 'b'])))
    assert not FS(lines(['a', 'c', 'b'])).issame(fsac)

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


# just for process pool
def _compute_groups_serial_as_list(*args: Any, **kwargs: Any) -> Iterable[Group]:
    return list(_compute_groups_serial(*args, **kwargs))

# todo these are already normalized paths?
# although then harder to handle exceptions... ugh
def _compute_groups_serial(
        paths: Sequence[Path],
        *,
        cleanup: Cleaner,
        diff_filter: Optional[str],
        config: Config,
        _wdir: Optional[Path],
) -> Iterable[Group]:
    assert len(paths) > 0

    IRes = Union[Exception, Cleaned]
    cleaned2orig: Dict[IRes, Path] = {}
    cleaned = []

    wdir: Path

    def iter_results() -> Iterator[IRes]:
        with ExitStack() as istack:
            # ugh. what a mess
            nonlocal wdir
            if _wdir is None:
                wdir = Path(istack.enter_context(TemporaryDirectory()))
            else:
                wdir = _wdir
            for i, p in enumerate(paths):
                logger.info('processing %s (%d/%d)', p, i, len(paths))

                res: IRes
                # ds = total_dir_size(wdir)
                # logger.debug('total wdir(%s) size: %s', wdir, ds)
                before = time()
                # pass it a unique dir so they don't mess up each other
                pwdir = Path(istack.enter_context(TemporaryDirectory(dir=wdir)))
                try:
                    res = istack.enter_context(cleanup(p, wdir=pwdir))
                except Exception as e:
                    logger.exception(e)
                    res = e
                after = time()
                logger.debug('cleanup(%s): took %.2f seconds', p, after - before)
                # TODO ugh. Exception isn't hashable in general, so at least assert to avoid ambiguity
                # not sure what would be the proper fix...
                assert res not in cleaned2orig, res
                cleaned2orig[res] = p
                cleaned.append(res)
                yield res


    def fset(*paths: Path) -> FileSet:
        return FileSet(paths, wdir=wdir)  # noqa: F821  # wdir is guaranteed to be initialized by iter_results

    def unlink_tmp_output(cleaned: Path) -> None:
        # meh. unlink is a bit manual, but bounds the filesystem use by two dumps
        orig = cleaned2orig[cleaned]
        if orig == cleaned:
            # handle 'identity' cleanup -- shouldn't try to remove user files
            return
        # meh... just in case
        assert str(cleaned.resolve()).startswith(str(Path(gettempdir()).resolve())), cleaned
        # todo no need to unlink in debug mode?
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
            # todo ugh... why are we using exception as a dict index??
            yield Group(
                items =[cleaned2orig[lfile]],
                pivots=[cleaned2orig[lfile]],
                error=True,
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
                        error=False,
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
                        # otherwise doesn't make sense?
                        assert config.prune_dominated

                        # in multiway mode we check if the boundaries (pivots) contain the rest
                        npivots = rstack.enter_context(fset(lpfile, right_res))
                        dominated = nitems.issubset(npivots, diff_filter=diff_filter)
                    else:
                        # in two-way mode we check if successive paths include each other
                        before_right = nitems.items[-2]
                        s1 = rstack.enter_context(fset(before_right))
                        s2 = rstack.enter_context(fset(right_res))

                        if not config.prune_dominated:
                            dominated = s1.issame(s2)
                        else:
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

    config = Config(multiway=multiway, prune_dominated=True)
    func = lambda paths: compute_groups(paths, cleanup=dummy, diff_filter=_FILTER_ALL_ADDED, config=config, _wdir=gwdir)

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
    config = Config(multiway=multiway, prune_dominated=True)
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
    for group in compute_groups(paths, cleanup=dummy, diff_filter=_FILTER_ALL_ADDED, config=config):
        groups.append(group)
    # shouldn't crash due to open files or something, at least
    expected = 399 if multiway else 799
    assert len(groups) == expected


@contextmanager
def _noop(path: Path, *, wdir: Path) -> Iterator[Path]:
    yield path


def test_special_characters(tmp_path: Path) -> None:
    p1 = tmp_path / 'p1'
    p1.write_text('A\n')
    p2 = tmp_path / 'p2'
    p2.write_text('A\n< C > whoops\n')
    p3 = tmp_path / 'p3'
    p3.write_text('A\n< C > whoops\n')
    p4 = tmp_path / 'p4'
    p4.write_text('A\n')

    config = Config(
        prune_dominated=True,
        multiway=True,
    )
    gg = [p1, p2, p3, p4]
    groups = list(compute_groups(
        gg,
        cleanup=_noop, config=config, diff_filter=_FILTER_ALL_ADDED,
    ))
    instructions = groups_to_instructions(groups, config=config)
    assert [type(i) for i in instructions] == [
        Keep,   # start of group
        Prune,  # same as next
        Keep,   # has unique item: < C > whoops
        Keep,   # end of group
    ]


@parametrize('multiway', [False, True])
def test_simple(multiway: bool, tmp_path: Path) -> None:
    config = Config(
        prune_dominated=True,
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
            cleanup=_noop, config=config, diff_filter=_FILTER_ALL_ADDED,
        ))
        instructions = groups_to_instructions(groups, config=config)
        assert [type(i) for i in instructions] == [Keep for _ in gg]


def test_filter(tmp_path: Path) -> None:
    config = Config(
        prune_dominated=False,
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


    groups = list(compute_groups(paths, cleanup=remove_all_except_a, config=config, diff_filter=_FILTER_ALL_ADDED))
    instructions = groups_to_instructions(groups, config=config)
    assert [type(i) for i in instructions] == [
        Keep,
        Prune,  # should prune because after filtering only A there is no difference in files
        Keep,
        Keep
    ]


def _prepare(tmp_path: Path):
    sets = [
        ['X'],                # keep
        ['B'],                # keep
        ['B'],                # prune (always, because it's the same)
        ['B'],                # prune if we prune dominated
        ['B', 'A'],           # prune if we prune dominated
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


@parametrize('prune_dominated', [
    True,
    False,
])
def test_twoway(tmp_path: Path, prune_dominated) -> None:
    paths = _prepare(tmp_path)

    config = Config(prune_dominated=prune_dominated, multiway=False)
    groups = list(compute_groups(paths, cleanup=_noop, config=config, diff_filter=_FILTER_ALL_ADDED))
    instructions = list(groups_to_instructions(groups, config=config))
    assert [type(i) for i in instructions] == [
        Keep,
        Keep,
        Prune,
        Prune if prune_dominated else Keep,  # dominated
        Prune if prune_dominated else Keep,  # dominated by the next set
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
        prune_dominated=True,
        multiway=True,
    )
    groups = list(compute_groups(paths, cleanup=_noop, diff_filter=_FILTER_ALL_ADDED, config=config))
    instructions = groups_to_instructions(groups, config=config)

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
        Prune ,  #
        Keep  ,  # in isolation, it's dominated by neighbours.. but if we prune it, we'll lose '33' permanently
        Prune ,  # dominated by neighbours
        Keep  ,  # always keep last
    ]


# todo config is unused here?
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
                deli = Prune(path=i, group=group)
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
                error=False,
            ) for p in ppp
        )
        res = groups_to_instructions(list(grit), config=config)
        return [(str(p.path), {Keep: 'keep', Prune: 'prune'}[type(p)]) for p in res]

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
        ('0', 'keep' ),
        ('a', 'keep' ),
        ('b', 'prune'),
        ('c', 'prune'),
        ('d', 'keep' ),
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
   #assert do(*inputs, config=Config(prune_dominated=True)) == [
   #    ('a', 'keep'  ),
   #    ('b', 'keep'  ),
   #    ('c', 'keep'  ),
   #    ('d', 'prune' ),
   #    ('e', 'prune' ),
   #    ('f', 'keep'  ),
   #    ('g', 'keep'  ),
   #    ('h', 'keep'  ),
   #]

    import pytest

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

def _cleanup_aux(path: Path, *, wdir: Path, Normaliser) -> ContextManager[Path]:
    n = Normaliser()
    return n.do_cleanup(path, wdir=wdir)


def compute_instructions(
        paths: Sequence[Path],
        *,
        Normaliser: Type[BaseNormaliser],
        threads: Optional[int],
) -> Iterator[Instruction]:
    import functools
    # meh.. makes sure it's picklable
    cleanup = functools.partial(_cleanup_aux, Normaliser=Normaliser)

    cfg = Config(
        prune_dominated=Normaliser.PRUNE_DOMINATED,
        multiway=Normaliser.MULTIWAY,
    )
    groups: Iterable[Group] = compute_groups(
        paths=paths,
        cleanup=cleanup,
        diff_filter=Normaliser._DIFF_FILTER,
        config=cfg,
        threads=threads,
    )
    instructions: Iterable[Instruction] = groups_to_instructions(groups, config=cfg)
    total = len(paths)
    # TODO eh. could at least dump dry mode stats here...
    done = 0
    for i, ins in enumerate(instructions):
        logger.debug(f'{i:<3}/{total:<3} %s : %s', ins.path, type(ins).__name__)
        yield ins
        done += 1
    assert done == len(paths)  # just in case


# FIXME add a test

from .common import Mode, Dry, Move, Remove
def apply_instructions(instructions: Iterable[Instruction], *, mode: Mode=Dry(), need_confirm: bool=True) -> NoReturn:
    import click

    # TODO hmm...
    # if we keep it as iterator, would be kinda nice, then it'd print cleaning stats as you run it
    # NOTE: will also need to remove (list) call in 'clean' subcommand
    totals: str
    if not isinstance(mode, Dry):
        # force for safety
        instructions = list(instructions)
        totals = f'{len(instructions):>3}'
    else:
        totals = '???'

    rm_action = {
        Dry   : click.style('REMOVE (dry mode)', fg='yellow'),
        Move  : click.style('MOVE             ', fg='yellow'),
        Remove: click.style('REMOVE           ', fg='red'   ),
    }[type(mode)]

    tot_files = 0
    rem_files = 0
    tot_bytes = 0
    rem_bytes = 0

    def stat() -> str:
        tmb = tot_bytes / 2 ** 20
        rmb = rem_bytes / 2 ** 20
        return f'pruned so far: {int(rmb):>4} Mb /{int(tmb):>4} Mb , {rem_files:>3} /{tot_files:>3} files'

    errored: List[Path] = []

    to_delete = []
    for idx, ins in enumerate(instructions):
        if ins.group.error:
            # TODO would be nice to print actual error or something.. but for now it's ok
            errored.append(ins.path)

        ip = ins.path
        sz = ip.stat().st_size
        tot_bytes += sz
        tot_files += 1
        action: str
        if   isinstance(ins, Keep):
            action = click.style('will keep        ', fg='green')
        elif isinstance(ins, Prune):
            action = rm_action
            rem_bytes += sz
            rem_files += 1
            to_delete.append(ins.path)
        else:
            raise RuntimeError(ins)
        logger.info(f'processing {idx:>4}/{totals:>4} %s : %s  ; %s', ip, action, stat())

    logger.info('SUMMARY: %s', stat())

    for e in errored:
        logger.error('error while processing %s', e)

    exit_code = 0 if len(errored) == 0 else 1

    if isinstance(mode, Dry):
        logger.info('dry mode! not touching anything')
        sys.exit(exit_code)

    from .utils import under_pytest
    assert not under_pytest  # just a paranoid check to prevent deleting something under tests by accident

    if len(to_delete) == 0:
        logger.info('no files to prune!')
        sys.exit(exit_code)

    if need_confirm and not click.confirm(f'Ready to {rm_action.strip().lower()} {len(to_delete)} files?', abort=True):
        sys.exit(exit_code)

    move_to: Optional[Path] = None
    if   isinstance(mode, Move):
        move_to = mode.path
        # just in case
        assert move_to.is_absolute(), move_to
    elif isinstance(mode, Remove):
        pass
    else:
        raise RuntimeError(mode, type(mode))

    for i in instructions:
        # just in case, to make sure no one messed with files in the meantime
        assert i.path.exists(), i.path

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

    sys.exit(exit_code)


def compute_diff(path1: Path, path2: Path, *, Normaliser) -> List[str]:
    # meh. copy pasted...
    def cleanup(path: Path, *, wdir: Path) -> ContextManager[Path]:
        n = Normaliser()
        return n.do_cleanup(path, wdir=wdir)

    from .processor import do_diff
    with TemporaryDirectory() as td1, TemporaryDirectory() as td2:
        with cleanup(path1, wdir=Path(td1)) as res1, cleanup(path2, wdir=Path(td2)) as res2:
            # ok, I guess diff_filter=None makes more sense here?
            # would mean it shows the whole thing
            # meh
            difftool = os.environ.get('DIFFTOOL', None)
            if difftool is not None:
                extras: List[str] = []
                if difftool == 'vimdiff':
                    wrap = ['-c', 'windo set wrap']
                    # wrap = []
                    diffopts = ['-c', 'set diffopt=filler,context:0']
                    extras.extend(wrap)
                    extras.extend(diffopts)

                os.execlp(difftool, difftool, *extras, str(res1), str(res2))
            return do_diff(res1, res2, diff_filter=None)
