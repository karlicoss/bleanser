from __future__ import annotations

import inspect
import shutil
import subprocess
import sys
import warnings
from collections.abc import Callable, Iterable, Iterator, Sequence
from concurrent.futures import Future, ProcessPoolExecutor
from contextlib import AbstractContextManager, ExitStack, contextmanager
from functools import cache
from pathlib import Path
from subprocess import check_call
from tempfile import NamedTemporaryFile, TemporaryDirectory, gettempdir
from time import time
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    NoReturn,
    Self,
    override,
)

import click
import more_itertools
from kompress import CPath, is_compressed

from .common import (
    Dry,
    Group,
    Instruction,
    Keep,
    Mode,
    Move,
    Prune,
    Remove,
    divide_by_size,
    logger,
)
from .ext.dummy_executor import DummyExecutor


@contextmanager
def bleanser_tmp_directory() -> Iterator[Path]:
    # TODO add a --tmp-dir cli setting and use it here?
    with TemporaryDirectory(prefix='bleanser') as tdir:
        yield Path(tdir)


# helper functions for normalisers
def unique_file_in_tempdir(*, input_filepath: Path, dir: Path, suffix: str | None = None) -> Path:  # noqa: A002
    '''
    this doesn't actually create the temp dir, dir is already made/cleaned up somewhere above

    say for a file like /home/user/data/something/else.json
    this creates a file like:

    /tmpdir/home/user/data/something/else-cleaned

    If suffix is not provided, it is NOT assumed/added automatically
    '''
    # make sure these are absolute
    input_filepath = input_filepath.absolute().resolve()
    assert dir.is_absolute()

    # is useful to keep the suffix the same, or let the user customize
    # in case some library code elsewhere a user might
    # use to process this uses the filetype to detect the filetype
    suffix = suffix or ''
    cleaned_dir = dir / Path(*input_filepath.parts[1:])
    cleaned_path = cleaned_dir / (input_filepath.stem + '-cleaned' + suffix)

    # FIXME vvv where is this happening?...
    # if this already exists somehow, use tempfile to add some random noise to the filename
    cleaned_path.parent.mkdir(parents=True, exist_ok=True)
    return cleaned_path


# meh... see Fileset._union
# this gives it a bit of a speedup when comparing
def sort_file(filepath: str | Path) -> None:
    check_call(['sort', '-o', str(filepath), str(filepath)])


Input = Path
Normalised = Path


class BaseNormaliser:
    ## user overridable configs
    PRUNE_DOMINATED: ClassVar[bool] = False
    MULTIWAY: ClassVar[bool] = False
    ##

    def __init__(self, *, original: Input, base_tmp_dir: Path) -> None:
        ## some sanity checks just in case
        assert original.is_absolute(), original
        assert original.exists(), original
        assert original.stat().st_size > 0, original
        ###

        """
        Original filepath, could possibly be compressed
        """
        self.original = original

        """
        "Base" temporary directory used by all of this Normaliser instances
        """
        # add the full class name for convenience while debugging
        self._base_tmp_dir = base_tmp_dir / self._relative_base_tmp_dir()

        """
        Temporary directory used during normalisation of one specific source file

        It's guaranteed to exist by the time "normalise" is called, and will be cleaned up after.
        So you can create temporary files in it if necessary without the need to clean up
        """
        without_root = Path(*self.original.parts[1:])
        self.tmp_dir = self._base_tmp_dir / without_root

    @classmethod
    def _relative_base_tmp_dir(cls) -> Path:
        mm = inspect.getmodule(cls)
        assert mm is not None
        spec = mm.__spec__
        assert spec is not None
        mname = spec.name
        parts = [*mname.split('.'), cls.__name__]
        rpath = Path(*parts)
        assert not rpath.is_absolute()  # just in case
        return rpath

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Normalised]:
        '''
        path: input file to clean up
              Note that it's not necessarily the same as self.original, e.g. if the original file was compressed

        subclasses would typically override this, reading from 'path' as the input file
        '''
        yield path

        # e.g., subclasses would read/parse data from path,
        # write to some unique tempfile (probably using unique_file_in_tempdir)
        # and yield that tempfile back to the caller
        #
        # for an example, see modules/json.py

    @contextmanager
    def do_normalise(self) -> Iterator[Normalised]:
        """
        This method does set up for normalise method, and generally shouldn't require overriding
        """
        self.tmp_dir.mkdir(parents=True)
        try:
            # FIXME write a test for compressed stuff
            with self.unpacked(path=self.original, wdir=self.tmp_dir) as unpacked:
                ## backwards compatibility -- do_cleanup used to take input path and tmp dir
                do_cleanup = getattr(self, 'do_cleanup', None)
                if do_cleanup is None:
                    with self.normalise(path=unpacked) as normalised:
                        yield normalised
                else:
                    warnings.warn(
                        "'do_cleanup' is deprecated. Remove wdir argument and rename it to 'normalise'", stacklevel=2
                    )
                    with do_cleanup(path=unpacked, wdir=self.tmp_dir) as normalised:
                        yield normalised
        finally:
            # ugh, kinda annoying that TemporaryDirectory doesn't allow creating a dir with exact name
            # so here we at least reuse its cleanup method
            TemporaryDirectory._rmtree(str(self.tmp_dir))  # type: ignore[attr-defined]

    if TYPE_CHECKING:
        # deliberately keep this during type checking to indicate users need to migrate to normalise()
        def do_cleanup(self) -> None: ...

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

        Then, in do_normalise, it uses the unpacked/extracted file from here
        '''
        if not is_compressed(path):
            # if not compressed, no need to create copies
            yield path
            return

        # todo ok, kinda annoying that a lot of time is spent unpacking xz files...
        # not sure what to do about it
        with CPath(str(path)).open(mode='rb') as fo:
            res = fo.read()

        # TODO maybe keep track of original files in the Normaliser and assert before removing anything
        # this would ensure the logic for using extra files is safe

        # TODO not sure if cleaned path _has_ to be in wdir? can we return the orig path?
        # maybe if the cleanup method is not implemented?
        cleaned_path = unique_file_in_tempdir(input_filepath=path, dir=wdir)
        cleaned_path.write_bytes(res)
        # writing to tmp does take a while... hmm
        yield cleaned_path

    @classmethod
    def main(cls) -> None:
        from .main import main as run_main

        run_main(Normaliser=cls)


def compute_groups(
    paths: Sequence[Path],
    *,
    Normaliser: type[BaseNormaliser],
    threads: int | None = None,
) -> Iterator[Group]:
    assert len(paths) == len(set(paths)), paths  # just in case
    assert len(paths) > 0  # just in case

    pool = DummyExecutor() if threads is None else ProcessPoolExecutor(max_workers=None if threads == 0 else threads)
    with pool, bleanser_tmp_directory() as base_tmp_dir:
        workers = getattr(pool, '_max_workers')
        workers = min(workers, len(paths))  # no point in using too many workers
        logger.info('using %d workers', workers)

        futures: list[Future] = []
        for paths_chunk in divide_by_size(buckets=workers, paths=paths):
            pp = list(paths_chunk)
            if len(pp) == 0:
                continue
            # force iterator if we're using more than one thread
            # otherwise it'll still be basically serial execution
            # in addition, multiprocess would fail to pickle returned iterator
            func: Callable[..., Iterable[Group]]
            # note: separate declaration and if statement makes mypy happy
            if threads is not None:
                func = _compute_groups_serial_as_list
            else:
                func = _compute_groups_serial
            futures.append(
                pool.submit(
                    func,
                    paths=pp,
                    Normaliser=Normaliser,
                    base_tmp_dir=base_tmp_dir,
                )
            )
        emitted: set[Path] = set()
        for f in futures:
            rit = f.result()
            for r in rit:
                emitted |= set(r.items)
                yield r
    assert emitted == set(paths), (paths, emitted)  # just in case


@cache
def _get_gnu_diff_binary() -> str:
    diff = 'diff'
    version = subprocess.check_output([diff, '--version'], text=True)
    # it's important to use consistent diff version for pruning
    # otherwise we might delete some files by accident
    assert 'GNU' in version, (
        version,
        "GNU diff isn't detected, make sure to run 'brew install diffutils' if you are on OSX",
    )
    return diff


def _subtract_files(lfile: Path, rfile: Path) -> str | None:
    """
    If lfile is fully contained in rfile, returns None. Otherwise returns the diff.
    """
    # Use custom group format to only show actual changes without the line number ranges/action codes
    #  %< means lines only in left file, %> means lines only in right file
    # Just in case, I benchmarked with hyperfine and seems like using custom formats makes no difference to speed.
    custom_diff_format = ["--changed-group-format=%<", "--unchanged-group-format="]

    res = subprocess.run(
        [_get_gnu_diff_binary(), lfile, rfile, *custom_diff_format], check=False, text=True, capture_output=True
    )
    if res.returncode not in (0, 1):
        res.check_returncode()

    if len(res.stdout) == 0:
        return None

    # TODO maybe log in a separate file
    # TODO not sure what's the best way to provide some quick debug means...
    # need grep -C or something like that...
    print_diff = True
    if print_diff:
        logger.debug('diff %s %s', lfile, rfile)
        logger.debug('vvvvvvvvvvvvv DIFF vvvvvvvvvvvvv')
        if sys.stdin.isatty():
            # only log when running interactively... otherwise spams syslog too much
            for line in res.stdout.splitlines():
                logger.debug(line)
        else:
            logger.debug('non-interactive session, skipping diff logging (otherwise spams syslog too much)')
        logger.debug('^^^^^^^^^^^^^ DIFF ^^^^^^^^^^^^^')

    return res.stdout


# TODO shit. it has to own tmp dir...
# we do need a temporary copy after all?
class FileSet(AbstractContextManager):
    def __init__(self, items: Sequence[Path] = (), *, wdir: Path) -> None:
        self.wdir = wdir
        self.items: list[Path] = []
        tfile = NamedTemporaryFile(dir=self.wdir, delete=False)  # noqa: SIM115
        self.merged = Path(tfile.name)
        self._union(*items)

    def _copy(self) -> FileSet:
        fs = FileSet(wdir=self.wdir)
        fs.items = list(self.items)
        shutil.copy(str(self.merged), str(fs.merged))
        return fs

    def union(self, *paths: Path) -> FileSet:
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
        for p in tomerge:  # todo no need to check self.merged?
            res = subprocess.run(['sort', '--check', p], check=False)
            if res.returncode not in (0, 1):
                res.check_returncode()
            is_sorted.append(res.returncode == 0)
        mflag = []
        if all(is_sorted):
            mflag = ['--merge']

        # sort also has --parallel option... but pretty pointless, in most cases we'll be merging two files?
        subprocess.check_call(['sort', '--unique', *mflag, *tomerge, '-o', self.merged])

        self.items.extend(extra)

    def issame(self, other: FileSet) -> bool:
        lfile = self.merged
        rfile = other.merged
        # TODO meh. maybe get rid of cmp, it's not really faster
        # even on exactly same file (copy) it seemed to be slower
        # https://unix.stackexchange.com/questions/153286/is-cmp-faster-than-diff-q
        res = subprocess.run(['cmp', '--silent', lfile, rfile], check=False)
        if res.returncode not in (0, 1):
            res.check_returncode()
        return res.returncode == 0

    def issubset(self, other: FileSet) -> bool:
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
        if self.issame(other):
            # short circuit
            return True

        difference = _subtract_files(lfile, rfile)
        return difference is None

    def __repr__(self) -> str:
        return repr((self.items, self.merged))

    @override
    def __enter__(self) -> Self:
        return self

    @override
    def __exit__(self, type, value, tb) -> None:
        self.close()

    def close(self) -> None:
        self.merged.unlink(missing_ok=True)


# just for process pool
def _compute_groups_serial_as_list(*args: Any, **kwargs: Any) -> Iterable[Group]:
    return list(_compute_groups_serial(*args, **kwargs))


type IRes = Exception | Normalised


# todo these are already normalized paths?
# although then harder to handle exceptions... ugh
def _compute_groups_serial(
    paths: Sequence[Path],
    *,
    Normaliser: type[BaseNormaliser],
    base_tmp_dir: Path,
) -> Iterable[Group]:
    assert len(paths) > 0

    cleaned2orig: dict[IRes, Path] = {}
    cleaned = []

    def iter_results() -> Iterator[IRes]:
        with ExitStack() as exit_stack:
            for idx, input in enumerate(paths):  # noqa: A001
                normaliser = Normaliser(original=input, base_tmp_dir=base_tmp_dir)

                logger.info('processing %s (%d/%d)', input, idx, len(paths))

                res: IRes
                # ds = total_dir_size(wdir)
                # logger.debug('total wdir(%s) size: %s', wdir, ds)
                before = time()
                try:
                    res = exit_stack.enter_context(normaliser.do_normalise())
                except Exception as e:
                    logger.exception(e)
                    res = e
                after = time()
                logger.debug('cleanup(%s): took %.2f seconds', input, after - before)
                # TODO ugh. Exception isn't hashable in general, so at least assert to avoid ambiguity
                # not sure what would be the proper fix...
                assert res not in cleaned2orig, res
                cleaned2orig[res] = input
                cleaned.append(res)
                yield res

    fileset_wdir = base_tmp_dir / 'fileset'
    fileset_wdir.mkdir(parents=True, exist_ok=True)

    def fset(*paths: Path) -> FileSet:
        return FileSet(paths, wdir=fileset_wdir)

    def unlink_tmp_output(cleaned: Path) -> None:
        # meh. unlink is a bit manual, but bounds the filesystem use by two dumps
        # todo maybe unlink whole tmp_dir for normaliser?
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

    ires[0]  # ugh. a bit crap, but we're nudging it to initialize wdir...

    left = 0
    # empty fileset is easier than optional
    items = fset()
    while left < total:
        lfile = ires[left]

        if isinstance(lfile, Exception):
            # todo ugh... why are we using exception as a dict index??
            # fmt: off
            yield Group(
                items =[cleaned2orig[lfile]],
                pivots=[cleaned2orig[lfile]],
                error=True,
            )
            # fmt: on
            left += 1
            continue

        items.close()
        items = fset(lfile)

        lpivot = left  # noqa: F841  # TODO why is this unused? maybe was meaning to use lpivot instead of left?
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

                def group(*, rm_last: bool) -> Group:
                    gitems = items.items
                    # fmt: off
                    citems  = [cleaned2orig[i] for i in gitems]
                    cpivots = [cleaned2orig[i] for i in pivots.items]
                    g = Group(
                        items =citems,
                        pivots=cpivots,
                        error=False,
                    )
                    # fmt: on
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

                next_state: tuple[FileSet, Path] | None
                if isinstance(right_res, Exception):
                    # short circuit... error itself will be handled when right_res is the leftmost element
                    next_state = None
                else:
                    nitems = items.union(right_res)

                    if Normaliser.MULTIWAY:
                        # otherwise doesn't make sense?
                        assert Normaliser.PRUNE_DOMINATED

                        # in multiway mode we check if the boundaries (pivots) contain the rest
                        npivots = rstack.enter_context(fset(lpfile, right_res))
                        dominated = nitems.issubset(npivots)
                    else:
                        # in two-way mode we check if successive paths include each other
                        before_right = nitems.items[-2]
                        s1 = rstack.enter_context(fset(before_right))
                        s2 = rstack.enter_context(fset(right_res))

                        if not Normaliser.PRUNE_DOMINATED:
                            dominated = s1.issame(s2)
                        else:
                            dominated = s1.issubset(s2)

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
                for i in items.items[1:-1]:
                    unlink_tmp_output(i)

                right += 1

    items.close()

    # meh. hacky but sort of does the trick
    cached = len(getattr(ires, '_cache'))
    assert cached == total, 'Iterator should be fully processed!'

    # TODO: this is not thread safe, should check this above the call stack when Pool is finished
    # stale_files = [p for p in base_tmp_dir.rglob('*') if p.is_file()]
    # TODO at the moment this assert fails sometimes -- need to investigate
    # assert len(stale_files) == 0, stale_files


# todo config is unused here?
def groups_to_instructions(groups: Iterable[Group]) -> Iterator[Instruction]:
    done: dict[Path, Instruction] = {}

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


def compute_instructions(
    paths: Sequence[Path],
    *,
    Normaliser: type[BaseNormaliser],
    threads: int | None,
) -> Iterator[Instruction]:
    groups: Iterable[Group] = compute_groups(
        paths=paths,
        Normaliser=Normaliser,
        threads=threads,
    )
    instructions: Iterable[Instruction] = groups_to_instructions(groups)
    total = len(paths)
    # TODO eh. could at least dump dry mode stats here...
    done = 0
    for i, ins in enumerate(instructions):
        logger.debug(f'{i:<3}/{total:<3} %s : %s', ins.path, type(ins).__name__)
        yield ins
        done += 1
    assert done == len(paths)  # just in case


def apply_instructions(
    instructions: Iterable[Instruction],
    *,
    mode: Mode = Dry(),  # noqa: B008
    need_confirm: bool = True,
) -> NoReturn:
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

    # fmt: off
    rm_action = {
        Dry   : click.style('REMOVE (dry mode)', fg='yellow'),
        Move  : click.style('MOVE             ', fg='yellow'),
        Remove: click.style('REMOVE           ', fg='red'   ),
    }[type(mode)]
    # fmt: on

    tot_files = 0
    rem_files = 0
    tot_bytes = 0
    rem_bytes = 0

    def stat() -> str:
        tmb = tot_bytes / 2**20
        rmb = rem_bytes / 2**20
        return f'pruned so far: {int(rmb):>4} Mb /{int(tmb):>4} Mb , {rem_files:>3} /{tot_files:>3} files'

    errored: list[Path] = []

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
        if isinstance(ins, Keep):
            action = click.style('will keep        ', fg='green')
        elif isinstance(ins, Prune):
            action = rm_action
            rem_bytes += sz
            rem_files += 1
            to_delete.append(ins.path)
        else:
            raise TypeError(ins)
        click.echo(f'processing {idx:>4}/{totals:>4} {ip} : {action}  ; {stat()}')

    click.echo(f'SUMMARY: {stat()}')

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

    move_to: Path | None = None
    if isinstance(mode, Move):
        move_to = mode.path
        # just in case
        assert move_to.is_absolute(), move_to
    elif isinstance(mode, Remove):
        pass
    else:
        raise TypeError(mode, type(mode))

    for i in instructions:
        # just in case, to make sure no one messed with files in the meantime
        assert i.path.exists(), i.path

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
