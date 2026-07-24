from pathlib import Path

from lxml import etree

from bleanser.core.modules.xml import Normaliser as XmlNormaliser

_PREFERENCES_PREFIX = 'com.simon.harmonichackernews.KEY_SHARED_PREFERENCES_'
_CACHED_STORY_PREFIX = _PREFERENCES_PREFIX + 'CACHED_STORY'
_CACHED_STORIES_KEY = _PREFERENCES_PREFIX + 'CACHED_STORIES_STRINGS'
_BOOKMARKS_KEY = _PREFERENCES_PREFIX + 'BOOKMARKS'
_DELIMITED_COLLECTION_KEYS = {
    _PREFERENCES_PREFIX + key
    for key in [
        'FAVORITES',
        'HISTORIES',
        'UPVOTED',
    ]
} | {_BOOKMARKS_KEY}


def _delimited_collection_values(child: etree._Element) -> list[str]:
    assert child.tag == 'string', child.tag
    assert child.text is not None, child
    return child.text.split('-')


class Normaliser(XmlNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, t: etree._Element) -> etree._Element:
        # Android shared preferences use a flat map, so another root would mean the input schema has changed.
        assert t.tag == 'map', t.tag

        # Cached JSON stays opaque, while the cache index is flattened below like any other set.
        # HPI interprets relationships between cache and bookmark records, but cleanup does not need to.

        for child in list(t):
            name = child.get('name')
            assert name is not None, child

            # Flatten collections so their entries are compared independently instead of as one changing XML value.
            values: list[str] | None = None
            if child.tag == 'set':
                values = []
                for item in child:
                    assert item.tag == 'string', item.tag
                    assert item.text is not None, item
                    values.append(item.text)
            elif name in _DELIMITED_COLLECTION_KEYS:
                # Harmonic stores these collection entries in one hyphen-delimited string.
                values = _delimited_collection_values(child)

            if values is not None:
                position = t.index(child)
                t.remove(child)
                for value in values:
                    item = etree.Element('string', name=name)
                    item.text = value
                    # The XML normaliser treats top-level lines as separate set records.
                    item.tail = '\n'
                    t.insert(position, item)
                    position += 1

        return t


if __name__ == '__main__':
    Normaliser.main()


def test_harmonic_android(tmp_path: Path) -> None:
    from bleanser.tests.common import actions

    def snapshot(
        name: str,
        *,
        cache_id: int,
        clicked_ids: list[int],
        bookmarks: list[str],
        cache_title: str | None = None,
    ) -> Path:
        path = tmp_path / name
        clicked = ''.join(f'<string>{clicked_id}</string>' for clicked_id in clicked_ids)
        cache_value = f'{{"id": {cache_id}}}'
        if cache_title is not None:
            cache_value = f'{{"id": {cache_id}, "title": "{cache_title}"}}'
        path.write_text(f'''
        <map>
            <set name="{_PREFERENCES_PREFIX}CLICKED_IDS">
                {clicked}
            </set>
            <string name="{_CACHED_STORY_PREFIX}{cache_id}">{cache_value}</string>
            <set name="{_CACHED_STORIES_KEY}">
                <string>{cache_id}-1234567890</string>
            </set>
            <string name="{_PREFERENCES_PREFIX}BOOKMARKS">{'-'.join(bookmarks)}</string>
        </map>
        ''')
        return path

    first = snapshot('first.xml', cache_id=101, clicked_ids=[1], bookmarks=['1q100'])
    second = snapshot('second.xml', cache_id=101, clicked_ids=[1, 2], bookmarks=['1q100', '2q200'])
    third = snapshot('third.xml', cache_id=101, clicked_ids=[2, 1, 3], bookmarks=['1q100', '2q200', '3q300'])
    removed = snapshot('removed.xml', cache_id=101, clicked_ids=[1, 3], bookmarks=['1q100', '3q300'])
    restored = snapshot('restored.xml', cache_id=101, clicked_ids=[1, 2, 3], bookmarks=['1q100', '2q200', '3q300'])

    res = actions(paths=[first, second, third, removed, restored], Normaliser=Normaliser)

    assert res.remaining == [first, third, restored]

    cached_first = snapshot('cached_first.xml', cache_id=1, clicked_ids=[1], bookmarks=['1q100'], cache_title='first')
    cached_changed = snapshot(
        'cached_changed.xml',
        cache_id=1,
        clicked_ids=[1],
        bookmarks=['1q100'],
        cache_title='changed',
    )
    cached_restored = snapshot(
        'cached_restored.xml',
        cache_id=1,
        clicked_ids=[1],
        bookmarks=['1q100'],
        cache_title='first',
    )

    cached_res = actions(paths=[cached_first, cached_changed, cached_restored], Normaliser=Normaliser)

    assert cached_res.remaining == [cached_first, cached_changed, cached_restored]
