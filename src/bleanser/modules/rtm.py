from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from bleanser.core.modules.extract import ExtractObjectsNormaliser


# iCalendar folds long content onto continuation lines beginning with a space or tab.
# Join those physical lines back into the original logical content line.
def _logical_lines(text: str) -> Iterator[str]:
    previous: str | None = None
    physical_lines = text.split('\n')
    if physical_lines[-1] == '':
        physical_lines.pop()
    for line in physical_lines:
        # RTM occasionally emits duplicated carriage returns before LF.
        line = line.rstrip('\r')
        if line.startswith((' ', '\t')):
            assert previous is not None
            previous += line[1:]
        else:
            if previous is not None:
                yield previous
            previous = line
    assert previous is not None
    yield previous


def _extract_top_level_components(text: str) -> Iterator[tuple[str, str]]:
    lines = iter(_logical_lines(text))
    assert next(lines) == 'BEGIN:VCALENDAR'

    calendar: list[str] = []
    active: str | None = None
    contents: list[str] = []
    todo_count = 0
    calendar_closed = False

    for line in lines:
        if active is None:
            if line == 'END:VCALENDAR':
                calendar_closed = True
                break
            if line == 'BEGIN:VTIMEZONE':
                # Preserve VTIMEZONE even though RTM regenerates it from the named TZID.
                # Its TZOFFSETFROM values can oscillate between task-identical exports.
                active = 'VTIMEZONE'
                contents = []
                continue
            if line == 'BEGIN:VTODO':
                active = 'VTODO'
                contents = []
                continue

            assert not line.startswith(('BEGIN:', 'END:')), line
            assert ':' in line, line
            calendar.append(line)
            continue

        if line == f'END:{active}':
            # The component type is kept in the tuple, so only its outer tags are omitted.
            yield active, '\n'.join(contents)
            if active == 'VTODO':
                todo_count += 1
            active = None
            continue

        assert line != f'BEGIN:{active}', line
        assert ':' in line, line
        contents.append(line)

    assert calendar_closed
    assert active is None
    for line in lines:
        assert line == ''
    assert todo_count > 0
    yield 'VCALENDAR', '\n'.join(calendar)


class Normaliser(ExtractObjectsNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def extract_objects(self, path: Path) -> Iterator[tuple[str, str]]:
        # Preserve physical line endings so _logical_lines can normalize RTM's occasional CRCRLF.
        # Otherwise universal-newline conversion turns CRCRLF into two logical lines.
        with path.open(newline='') as stream:
            yield from _extract_top_level_components(stream.read())


if __name__ == '__main__':
    Normaliser.main()


def test_rtm(tmp_path: Path) -> None:
    import pytest

    from bleanser.tests.common import actions

    def calendar(*, offset: str, todos: list[list[str]]) -> str:
        lines = [
            'BEGIN:VCALENDAR',
            'VERSION:2.0',
            'PRODID:-//RTM test//EN',
            'CALSCALE:GREGORIAN',
            'BEGIN:VTIMEZONE',
            'TZID:Europe/London',
            'BEGIN:STANDARD',
            f'TZOFFSETFROM:{offset}',
            'END:STANDARD',
            'END:VTIMEZONE',
        ]
        for todo in todos:
            lines.extend(['BEGIN:VTODO', *todo, 'END:VTODO'])
        lines.append('END:VCALENDAR')
        return '\r\n'.join(lines) + '\r\n'

    first = tmp_path / 'first.ical'
    first.write_text(calendar(offset='+0000', todos=[['UID:one', 'SUMMARY:a long task']]))
    middle = tmp_path / 'middle.ical'
    middle.write_text(calendar(offset='+0000', todos=[['UID:one', 'SUMMARY:a long', '  task']]))
    last = tmp_path / 'last.ical'
    last.write_text(calendar(offset='+0000', todos=[['UID:one', 'SUMMARY:a long task']]))

    result = actions(paths=[first, middle, last], Normaliser=Normaliser)
    assert result.pruned == [middle]

    different_timezone = tmp_path / 'different-timezone.ical'
    different_timezone.write_text(calendar(offset='+0100', todos=[['UID:one', 'SUMMARY:a long task']]))
    result = actions(paths=[first, different_timezone, last], Normaliser=Normaliser)
    assert result.pruned == []

    duplicated_carriage_return = tmp_path / 'duplicated-carriage-return.ical'
    duplicated_carriage_return.write_text(calendar(offset='+0000', todos=[['UID:one', 'SUMMARY:a long task\r']]))
    result = actions(paths=[first, duplicated_carriage_return, last], Normaliser=Normaliser)
    assert result.pruned == [duplicated_carriage_return]

    expected = list(_extract_top_level_components(calendar(offset='+0000', todos=[['UID:one', 'SUMMARY:a long task']])))
    trailing_blank_line = calendar(offset='+0000', todos=[['UID:one', 'SUMMARY:a long task']]) + '\r\n'
    assert list(_extract_top_level_components(trailing_blank_line)) == expected

    components = list(
        _extract_top_level_components(
            calendar(
                offset='+0000',
                todos=[
                    ['UID:one', 'SUMMARY:a long task'],
                    ['UID:two', 'BEGIN:VALARM', 'ACTION:DISPLAY', 'END:VALARM'],
                ],
            )
        )
    )
    assert components == [
        ('VTIMEZONE', 'TZID:Europe/London\nBEGIN:STANDARD\nTZOFFSETFROM:+0000\nEND:STANDARD'),
        ('VTODO', 'UID:one\nSUMMARY:a long task'),
        ('VTODO', 'UID:two\nBEGIN:VALARM\nACTION:DISPLAY\nEND:VALARM'),
        ('VCALENDAR', 'VERSION:2.0\nPRODID:-//RTM test//EN\nCALSCALE:GREGORIAN'),
    ]

    malformed = calendar(offset='+0000', todos=[['UID:one', 'missing separator']])
    with pytest.raises(AssertionError, match='missing separator'):
        list(_extract_top_level_components(malformed))

    unknown_nested_component = calendar(
        offset='+0000',
        todos=[['UID:one', 'BEGIN:VUNKNOWN', 'USEFUL:data', 'END:VUNKNOWN']],
    )
    components = list(_extract_top_level_components(unknown_nested_component))
    assert ('VTODO', 'UID:one\nBEGIN:VUNKNOWN\nUSEFUL:data\nEND:VUNKNOWN') in components

    unknown_top_level_component = calendar(offset='+0000', todos=[]).replace(
        'BEGIN:VTIMEZONE',
        'BEGIN:VUNKNOWN',
    )
    with pytest.raises(AssertionError, match='BEGIN:VUNKNOWN'):
        list(_extract_top_level_components(unknown_top_level_component))
