from typing import NoReturn

def assert_never(value: NoReturn) -> NoReturn:
    assert False, f'Unhandled value: {value} ({type(value).__name__})'
