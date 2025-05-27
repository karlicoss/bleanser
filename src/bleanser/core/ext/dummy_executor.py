from __future__ import annotations

from concurrent.futures import Executor, Future

# https://stackoverflow.com/a/10436851/706389
from typing import Any


class DummyExecutor(Executor):
    def __init__(self, max_workers: int | None = 1) -> None:
        self._shutdown = False
        self._max_workers = max_workers

    def submit(self, fn, *args, **kwargs):  # type: ignore[override,unused-ignore]  # todo type properly after 3.9
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

    def shutdown(self, wait: bool = True, **kwargs) -> None:  # noqa: FBT001,FBT002,ARG002
        self._shutdown = True
