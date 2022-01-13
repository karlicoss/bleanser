from typing import NoReturn
def assert_never(value: NoReturn) -> NoReturn:
    assert False, f'Unhandled value: {value} ({type(value).__name__})'


from pathlib import Path
def total_dir_size(d: Path) -> int:
    return sum(f.stat().st_size for f in d.glob('**/*') if f.is_file())


import sys
under_pytest = 'pytest' in sys.modules
### ugh. pretty horrible... but
# 'PYTEST_CURRENT_TEST' in os.environ
# doesn't work before we're actually inside the test.. and it might be late for decorators, for instance
###


import time

class Timer:
    def __init__(self, *tags):
        self.tags = tags

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args):
        self.end = time.time()
        delta = self.end - self.start
        print(f"{self.tags} TIME TAKEN: {delta:.1f}", file=sys.stderr)


from functools import wraps

def timing(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        with Timer(f.__name__):
            return f(*args, **kwargs)
    return wrapped


# make it lazy, otherwise it might crash on module import (e.g. on Windows)
# ideally would be nice to fix it properly https://github.com/ahupp/python-magic#windows
from functools import lru_cache
import warnings
from typing import Callable, Optional
@lru_cache(1)
def _magic() -> Callable[[Path], Optional[str]]:
    try:
        import magic # type: ignore
    except Exception as e:
        # logger.exception(e)
        defensive_msg: Optional[str] = None
        if isinstance(e, ModuleNotFoundError) and e.name == 'magic':
            defensive_msg = "python-magic is not detected. It's recommended for better file type detection (pip3 install --user python-magic). See https://github.com/ahupp/python-magic#installation"
        elif isinstance(e, ImportError):
            emsg = getattr(e, 'msg', '') # make mypy happy
            if 'failed to find libmagic' in emsg: # probably the actual library is missing?...
                defensive_msg = "couldn't import magic. See https://github.com/ahupp/python-magic#installation"
        if defensive_msg is not None:
            warnings.warn(defensive_msg)
            return lambda path: None # stub
        else:
            raise e
    else:
        mm = magic.Magic(mime=True)
        return lambda path: mm.from_file(str(path))


def mime(path: Path) -> Optional[str]:
    # next, libmagic, it might access the file, so a bit slower
    magic = _magic()
    return magic(path)


from typing import Any
Json = Any


from typing import Union, Collection
def delkeys(j: Json, *, keys: Union[str, Collection[str]]) -> None:
    if isinstance(keys, str):
        keys = {keys} # meh

    # todo if primitive, don't do anything
    if   isinstance(j, (int, float, bool, type(None), str)):
        return
    elif isinstance(j, list):
        for v in j:
            delkeys(v, keys=keys)
    elif isinstance(j, dict):
        for key in keys:
            j.pop(key, None)
        for k, v in j.items():
            delkeys(v, keys=keys)
    else:
        raise RuntimeError(type(j))
