from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from lxml import etree

from bleanser.core.processor import (
    BaseNormaliser,
    Normalised,
    sort_file,
    unique_file_in_tempdir,
)


class Normaliser(BaseNormaliser):
    PRUNE_DOMINATED = False

    def cleanup(self, t: etree._Element) -> etree._Element:
        return t

    @contextmanager
    def normalise(self, *, path: Path) -> Iterator[Normalised]:
        # todo not sure if need to release some resources here...
        parser = etree.XMLParser(remove_blank_text=True)
        # TODO we seem to lose comments here... meh
        et = etree.fromstring(path.read_bytes(), parser=parser)
        # restore newlines just for the top level
        assert et.text is None, et.text
        et.text = '\n'
        for c in et:
            assert c.tail is None, c.tail
            c.tail = '\n'

        et = self.cleanup(et)

        cleaned = unique_file_in_tempdir(input_filepath=path, dir=self.tmp_dir, suffix='.xml')
        cleaned.write_text(etree.tostring(et, encoding="unicode"))

        # TODO what is the assumption about shape?
        # either list of xml entries
        # or top-level thing with children

        # todo meh... see Fileset._union
        # this gives it a bit of a speedup
        sort_file(cleaned)
        yield cleaned


if __name__ == '__main__':
    Normaliser.main()


def test_xml_simple(tmp_path: Path) -> None:
    from bleanser.tests.common import actions, hack_attribute

    f1 = tmp_path / 'f1'
    f2 = tmp_path / 'f2'
    f3 = tmp_path / 'f3'
    f4 = tmp_path / 'f4'

    # make sure it handles
    f1.write_text('''
    <root>
    <x>text1</x>
    <x>text2</x>
    </root>
    ''')

    f2.write_text('''
    <root>
    <x>text2</x>
    <x>text3</x>
    <x>text4</x>
    </root>
    ''')

    f3.write_text('''
    <root>
   <x>text4</x>
   <x>text5</x>
    </root>
    ''')

    # note: we don't care about order
    f4.write_text('''
    <root>
    <x>text5</x>
    <x>text4</x>
    <x>text3</x>
    <x>text2</x>
    </root>
    ''')

    paths123 = [f1, f2, f3]
    with hack_attribute(Normaliser, 'MULTIWAY', value=True), hack_attribute(Normaliser, 'PRUNE_DOMINATED', value=True):
        res123 = actions(paths=paths123, Normaliser=Normaliser)
    assert res123.remaining == paths123

    paths124 = [f1, f2, f4]
    with hack_attribute(Normaliser, 'MULTIWAY', value=True), hack_attribute(Normaliser, 'PRUNE_DOMINATED', value=True):
        res124 = actions(paths=paths124, Normaliser=Normaliser)
    assert res124.remaining == [
        f1,
        f4,
    ]


def test_xml_nested(tmp_path: Path) -> None:
    from bleanser.tests.common import actions, hack_attribute

    f1 = tmp_path / 'f1'
    f2 = tmp_path / 'f2'
    f3 = tmp_path / 'f3'
    # make sure we don't just sort all lines and treat them as set
    # this could happen if you just pretty print the whole structure and diff
    # TODO: tbh this is also a good test for 'simple' handling
    f1.write_text('''
<root>
<item>
    <a val="1"></a>
    <b val="2"></b>
</item>
<item>
    <a val="2"></a>
    <b val="3"></b>
</item>
<item>
    <a val="1"></a>
    <b val="3"></b>
</item>
</root>
    ''')
    f2.write_text('''
<root>
<item>
    <a val="1"></a>
    <b val="1"></b>
</item>
<item>
    <a val="2"></a>
    <b val="2"></b>
</item>
<item>
    <a val="3"></a>
    <b val="3"></b>
</item>
</root>
    ''')
    f3.write_text('''
<root>
<item>
    <a val="1"></a>
    <b val="3"></b>
</item>
<item>
    <a val="2"></a>
    <b val="1"></b>
</item>
<item>
    <a val="3"></a>
    <b val="1"></b>
</item>
</root>
    ''')

    paths = [f1, f2, f3]
    with hack_attribute(Normaliser, 'MULTIWAY', value=True), hack_attribute(Normaliser, 'PRUNE_DOMINATED', value=True):
        res = actions(paths=paths, Normaliser=Normaliser)
    assert res.remaining == [
        f1,
        f2,
        f3,
    ]
