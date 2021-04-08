from typing import NoReturn
def assert_never(value: NoReturn) -> NoReturn:
    assert False, f'Unhandled value: {value} ({type(value).__name__})'


from sqlite3 import Connection
from typing import List
def get_tables(c: Connection) -> List[str]:
    cur = c.execute('SELECT name FROM sqlite_master')
    names = [c[0] for c in cur]
    return names


# https://stackoverflow.com/a/10436851/706389
from typing import Any, Optional
from concurrent.futures import Future, Executor
class DummyExecutor(Executor):
    def __init__(self, max_workers: Optional[int]=1) -> None:
        self._shutdown = False
        self._max_workers = max_workers

    def submit(self, fn, *args, **kwargs) -> Future:  # type: ignore[override]
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')

        f: Future[Any] = Future()
        try:
            result = fn(*args, **kwargs)
        except KeyboardInterrupt:
            raise
        except BaseException as e:
            f.set_exception(e)
        else:
            f.set_result(result)

        return f

    def shutdown(self, wait: bool=True) -> None:  # type: ignore[override]
        self._shutdown = True


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
from typing import Callable
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
