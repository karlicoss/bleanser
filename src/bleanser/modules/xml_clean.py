#!/usr/bin/env python3
"""
Ugh, wtf?? If I name it simply 'xml', I get all sorts of weird behaviours... presumably because it conflicts with some system modules..
"""

from lxml import etree

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


from bleanser.core.processor import BaseNormaliser


class Normaliser(BaseNormaliser):
    PRUNE_DOMINATED = False

    def cleanup(self, t: etree._Element) -> etree._Element:
        return t

    @contextmanager
    def do_cleanup(self, path: Path, *, wdir: Path) -> Iterator[Path]:
        assert path.stat().st_size > 0, path  # just in case

        with self.unpacked(path=path, wdir=wdir) as upath:
            pass
        del path # just to prevent from using by accident

        # todo not sure if need to release some resources here...
        parser = etree.XMLParser(remove_blank_text=True)
        # TODO we seem to lose comments here... meh
        et = etree.fromstring(upath.read_bytes(), parser=parser)
        # restore newlines just for the top level
        assert et.text is None, et.text
        et.text = '\n'
        for c in et:
            assert c.tail is None, c.tail
            c.tail = '\n'

        et = self.cleanup(et)

        # todo copy paste from SqliteNormaliser
        jpath = upath.absolute().resolve()
        cleaned = wdir / Path(*jpath.parts[1:]) / (jpath.name + '-cleaned')
        cleaned.parent.mkdir(parents=True, exist_ok=True)
        cleaned.write_text(etree.tounicode(et))

        # TODO what is the assumption about shape?
        # either list of xml entries
        # or top-level thing with children

        # todo meh... see Fileset._union
        # this gives it a bit of a speedup
        from subprocess import check_call
        check_call(['sort', '-o', cleaned, cleaned])
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
    with hack_attribute(Normaliser, 'MULTIWAY', True), hack_attribute(Normaliser, 'PRUNE_DOMINATED', True):
        res123 = actions(paths=paths123, Normaliser=Normaliser)
    assert res123.remaining == paths123

    paths124 = [f1, f2, f4]
    with hack_attribute(Normaliser, 'MULTIWAY', True), hack_attribute(Normaliser, 'PRUNE_DOMINATED', True):
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
    with hack_attribute(Normaliser, 'MULTIWAY', True), hack_attribute(Normaliser, 'PRUNE_DOMINATED', True):
        res = actions(paths=paths, Normaliser=Normaliser)
    assert res.remaining == [
        f1,
        f2,
        f3,
    ]
