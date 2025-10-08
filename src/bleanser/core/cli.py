from __future__ import annotations

import glob as stdlib_glob
import importlib
import os
from pathlib import Path
from typing import cast

import click
from more_itertools import zip_equal

from .common import Dry, Instruction, Keep, Mode, Move, Prune, Remove, logger
from .processor import BaseNormaliser, apply_instructions, bleanser_tmp_directory, compute_diff, compute_instructions


@click.group(context_settings={'max_content_width': 120, 'show_default': True})
def main() -> None:
    pass


def _get_Normalisers(*, ctx: click.Context, normalisers: list[str]) -> list[type[BaseNormaliser]]:
    via_ctx = []
    if ctx.obj is not None:
        Normaliser = ctx.obj['normaliser']
        via_ctx.append(Normaliser)

    if len(via_ctx) > 0:
        if len(normalisers) > 0:
            raise RuntimeError("""
You can't specify --normaliser when using bleanser as python3 -m bleanser.modules.<module> ....
""")
        return via_ctx

    if len(normalisers) > 0:
        assert len(via_ctx) == 0, via_ctx
        return [cast(type[BaseNormaliser], getattr(importlib.import_module(n), 'Normaliser')) for n in normalisers]

    raise RuntimeError("Should not happen")


# TODO link to documentation for multiple normalisers?
option_normaliser = click.option(
    '--normaliser',
    type=str,
    multiple=True,
    help='''
Normaliser module(s) to use, i.e. --normaliser bleanser.modules.twitter_android --normaliser bleanser.module.hpi.twitter_android.

Using multiple normalisers is an advanced/experimental feature, will be documented later.
''',
)


@main.command(name='prune', short_help='process & prune files')
@option_normaliser
@click.argument('path', type=str)
@click.option('--glob', is_flag=True, default=False, help='Treat the path as glob (in the glob.glob sense)')
@click.option('--sort-by', type=click.Choice(['size', 'name']), default='name', help='how to sort input files')
##
@click.option(
    '--dry',
    is_flag=True,
    default=None,
    help='Do not prune the input files, just print what would happen after pruning.',
)
@click.option('--remove', is_flag=True, default=None, help='Prune the input files by REMOVING them (be careful!)')
@click.option(
    '--move',
    type=Path,
    help='Prune the input files by MOVING them to the specified path. A bit safer than --remove mode.',
)
##
@click.option('--yes', is_flag=True, default=False, help="Do not prompt before pruning files (useful for cron etc)")
@click.option(
    '--threads',
    type=int,
    is_flag=False,
    flag_value=0,
    default=None,
    help="Number of threads (processes) to use. Without the flag won't use any, with the flag will try using all available, can also take a specific value. Passed down to PoolExecutor.",
)
##
@click.option('--from', 'from_', type=int, default=None)
@click.option('--to', type=int, default=None)
##
@click.option('--multiway', is_flag=True, default=None, help='force "multiway" cleanup')
@click.option('--prune-dominated', is_flag=True, default=None)
@click.pass_context
def prune(
    ctx: click.Context,
    *,
    normaliser: list[str],
    path: str,
    sort_by: str,
    glob: bool,
    dry: bool,
    move: Path | None,
    remove: bool,
    threads: int | None,
    from_: int | None,
    to: int | None,
    multiway: bool | None,
    prune_dominated: bool | None,
    yes: bool,
) -> None:
    Normalisers = _get_Normalisers(ctx=ctx, normalisers=normaliser)

    modes: list[Mode] = []
    if dry is True:
        modes.append(Dry())
    if move is not None:
        modes.append(Move(path=move))
    if remove is True:
        modes.append(Remove())
    if len(modes) == 0:
        modes.append(Dry())
    assert len(modes) == 1, f'please specify exactly one of modes (got {modes})'
    [mode] = modes
    # TODO eh, would be nice to use some package for mutually exclusive args..
    # e.g. https://stackoverflow.com/questions/37310718/mutually-exclusive-option-groups-in-python-click

    paths = _get_paths(path=path, glob=glob, from_=from_, to=to, sort_by=sort_by)

    if multiway is not None:
        for Normaliser in Normalisers:
            Normaliser.MULTIWAY = multiway
    if prune_dominated is not None:
        for Normaliser in Normalisers:
            Normaliser.PRUNE_DOMINATED = prune_dominated

    # TODO maybe move this logic insode apply_instructions?
    # then can print instructions for different normalisers
    all_instructions: list[list[Instruction]] = []
    for Normaliser in Normalisers:
        instructions = list(compute_instructions(paths, Normaliser=Normaliser, threads=threads))
        all_instructions.append(instructions)

    for path_instructions in zip_equal(*all_instructions):
        # just in case
        assert len({i.path for i in path_instructions}) == 1, path_instructions

        # sanity check:
        #   normalisers go from the most agnostic to the least agnosic
        #   so if the one on the 'left' says we need to prune, the one on the 'right' should also prune
        for i in range(len(Normalisers) - 1):
            il = path_instructions[i]
            ir = path_instructions[i + 1]
            if isinstance(il, Prune) and isinstance(ir, Keep):
                raise RuntimeError(  # noqa: TRY004
                    f"Inconsistent normalisers! {il.path} is pruned by {Normalisers[i]} but kept by {Normalisers[i + 1]}"
                )

    # for actual pruning, use the least 'agnostic'/most efficient normaliser
    instructions = all_instructions[-1]

    # NOTE: for now, forcing list() to make sure instructions compute before path check
    # not strictly necessary
    for p in paths:
        # just in case, to make sure no one messed with files in the meantime
        assert p.exists(), p

    need_confirm = not yes
    apply_instructions(instructions, mode=mode, need_confirm=need_confirm)


# meh... would be nice to use object but it gets casted to str by click??
_DEFAULT = '<default>'


@main.command(name='diff', short_help='cleanup two files and diff')
@option_normaliser
@click.argument('path1', type=str)
@click.argument('path2', default=_DEFAULT)
@click.option('--glob', is_flag=True, default=False, help='Treat the path as glob (in the glob.glob sense)')
@click.option('--vim', is_flag=True, default=False, help='Use vimdiff')
@click.option('--difftool', type=str, help='Custom difftool to use')
@click.option('--from', 'from_', type=int, default=None)
@click.option('--to', type=int, default=None, help='non-inclusive, i.e. [from, to)')
def diff(
    ctx: click.Context,
    *,
    normaliser: list[str],
    path1: str,
    path2: Path,
    glob: bool,
    from_: int | None,
    to: int | None,
    vim: bool,
    difftool: str,
) -> None:
    Normalisers = _get_Normalisers(ctx=ctx, normalisers=normaliser)
    [Normaliser] = Normalisers

    path1_: Path
    if glob:
        assert path2 is cast(Path, _DEFAULT), path2
        if to is None:
            assert from_ is not None
            to = from_ + 2  # by default just compare with the next adjacent element
        paths = _get_paths(path=path1, from_=from_, to=to, glob=glob)
    else:
        assert cast(str, path2) is not _DEFAULT
        assert from_ is None
        assert to is None
        path1_ = Path(path1)
        path2 = Path(path2)
        paths = [path1_, path2]

    # meh..
    if vim:
        difftool = 'vimdiff'
    if difftool is not None:
        os.environ['DIFFTOOL'] = difftool

    for line in compute_diff(paths, Normaliser=Normaliser):
        print(line)


@main.command(name='normalised', short_help='normalise file and dump to stdout')
@option_normaliser
@click.argument('path', type=Path)
@click.option('--stdout', is_flag=True, help='print normalised files to stdout instead of printing the path to it')
def normalised(ctx: click.Context, *, normaliser: list[str], path: Path, stdout: bool) -> None:
    Normalisers = _get_Normalisers(ctx=ctx, normalisers=normaliser)
    [Normaliser] = Normalisers
    with bleanser_tmp_directory() as base_tmp_dir:
        n = Normaliser(original=path, base_tmp_dir=base_tmp_dir)
        with n.do_normalise() as cleaned:
            if stdout:
                print(cleaned.read_text())
            else:
                click.secho(f'You can examine normalised file: {cleaned}', fg='green')
                click.pause(info="Press any key when you've finished")


def _get_paths(
    *, path: str, from_: int | None, to: int | None, sort_by: str = "name", glob: bool = False
) -> list[Path]:
    if not glob:
        pp = Path(path)
        assert pp.is_dir(), pp
        path = str(pp) + os.sep + '**'
    paths = [Path(p) for p in stdlib_glob.glob(path, recursive=True)]  # noqa: PTH207
    paths = [p for p in paths if p.is_file()]
    if sort_by == "name":
        # assumes sort order is same as date order? guess it's reasonable
        paths = sorted(paths)
    else:
        paths = sorted(paths, key=lambda s: s.stat().st_size)

    if from_ is None:
        from_ = 0
    if to is None:
        to = len(paths)
    paths = paths[from_:to]
    assert len(paths) > 0

    logger.info('processing %d files (%s ... %s)', len(paths), paths[0], paths[-1])
    return paths
