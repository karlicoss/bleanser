class CmpResult(Enum):
    DIFFERENT = 'different'
    SAME      = 'same'
    DOMINATES = 'dominates'
    ERROR     = 'error'


class Diff(NamedTuple):
    cmp: CmpResult
    diff: bytes


class Relation(NamedTuple):
    before: Path
    diff: Diff
    after: Path
