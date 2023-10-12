"""
Various helpers for compression
"""
from __future__ import annotations

from datetime import datetime
import pathlib
from pathlib import Path
import sys
from typing import Union, IO, Sequence, Any, Iterator
import io

PathIsh = Union[Path, str]


class Ext:
    xz    = '.xz'
    zip   = '.zip'
    lz4   = '.lz4'
    zstd  = '.zstd'
    zst   = '.zst'
    targz = '.tar.gz'


def is_compressed(p: Path) -> bool:
    # todo kinda lame way for now.. use mime ideally?
    # should cooperate with kompress.kopen?
    return any(p.name.endswith(ext) for ext in {Ext.xz, Ext.zip, Ext.lz4, Ext.zstd, Ext.zst, Ext.targz})


def _zstd_open(path: Path, *args, **kwargs) -> IO:
    import zstandard as zstd # type: ignore
    fh = path.open('rb')
    dctx = zstd.ZstdDecompressor()
    reader = dctx.stream_reader(fh)

    mode = kwargs.get('mode', 'rt')
    if mode == 'rb':
        return reader
    else:
        # must be text mode
        kwargs.pop('mode') # TextIOWrapper doesn't like it
        return io.TextIOWrapper(reader, **kwargs) # meh


# TODO use the 'dependent type' trick for return type?
def kopen(path: PathIsh, *args, mode: str='rt', **kwargs) -> IO:
    # just in case, but I think this shouldn't be necessary anymore
    # since when we can .read_text, encoding is passed already
    if mode in {'r', 'rt'}:
        encoding = kwargs.get('encoding', 'utf8')
    else:
        encoding = None
    kwargs['encoding'] = encoding

    pp = Path(path)
    name = pp.name
    if name.endswith(Ext.xz):
        import lzma

        # ugh. for lzma, 'r' means 'rb'
        # https://github.com/python/cpython/blob/d01cf5072be5511595b6d0c35ace6c1b07716f8d/Lib/lzma.py#L97
        # whereas for regular open, 'r' means 'rt'
        # https://docs.python.org/3/library/functions.html#open
        if mode == 'r':
            mode = 'rt'
        kwargs['mode'] = mode
        return lzma.open(pp, *args, **kwargs)
    elif name.endswith(Ext.zip):
        # eh. this behaviour is a bit dodgy...
        from zipfile import ZipFile
        zfile = ZipFile(pp)

        [subpath] = args # meh?

        ## oh god... https://stackoverflow.com/a/5639960/706389
        ifile = zfile.open(subpath, mode='r')
        ifile.readable = lambda: True  # type: ignore
        ifile.writable = lambda: False # type: ignore
        ifile.seekable = lambda: False # type: ignore
        ifile.read1    = ifile.read    # type: ignore
        # TODO pass all kwargs here??
        # todo 'expected "BinaryIO"'??
        return io.TextIOWrapper(ifile, encoding=encoding)
    elif name.endswith(Ext.lz4):
        import lz4.frame # type: ignore
        return lz4.frame.open(str(pp), mode, *args, **kwargs)
    elif name.endswith(Ext.zstd) or name.endswith(Ext.zst):
        kwargs['mode'] = mode
        return _zstd_open(pp, *args, **kwargs)
    elif name.endswith(Ext.targz):
        import tarfile
        # FIXME pass mode?
        tf = tarfile.open(pp)
        # TODO pass encoding?
        x = tf.extractfile(*args); assert x is not None
        return x
    else:
        return pp.open(mode, *args, **kwargs)


import typing
import os

if typing.TYPE_CHECKING:
    # otherwise mypy can't figure out that BasePath is a type alias..
    BasePath = pathlib.Path
else:
    BasePath = pathlib.WindowsPath if os.name == 'nt' else pathlib.PosixPath


class CPath(BasePath):
    """
    Hacky way to support compressed files.
    If you can think of a better way to do this, please let me know! https://github.com/karlicoss/HPI/issues/20

    Ugh. So, can't override Path because of some _flavour thing.
    Path only has _accessor and _closed slots, so can't directly set .open method
    _accessor.open has to return file descriptor, doesn't work for compressed stuff.
    """
    def open(self, *args, **kwargs):
        # TODO assert read only?
        return kopen(str(self), *args, **kwargs)
    # FIXME bytes mode...


open = kopen # TODO deprecate


# meh
def kexists(path: PathIsh, subpath: str) -> bool:
    try:
        kopen(path, subpath)
        return True
    except Exception:
        return False
