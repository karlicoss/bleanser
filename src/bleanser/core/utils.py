from typing import NoReturn
def assert_never(value: NoReturn) -> NoReturn:
    assert False, f'Unhandled value: {value} ({type(value).__name__})'


from sqlite3 import Connection
from typing import List
def get_tables(c: Connection) -> List[str]:
    cur = c.execute('SELECT name FROM sqlite_master')
    names = [c[0] for c in cur]
    names.remove('sqlite_sequence') # hmm not sure about it
    return names
