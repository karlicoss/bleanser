from bleanser.core.modules.xml import Normaliser as XmlNormaliser


class Normaliser(XmlNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def cleanup(self, t):
        # volatile attributes
        del t.attrib['count']
        del t.attrib['backup_date']
        del t.attrib['backup_set']
        return t


if __name__ == '__main__':
    Normaliser.main()


def test_smscalls() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions

    data = TESTDATA / 'smscalls'
    paths = sorted(data.glob('*.xml*'))

    res = actions(paths=paths, Normaliser=Normaliser)

    assert [p.name for p in res.remaining] == [
        'calls-20161211023623.xml',
        'calls-20161218221620.xml',
        'calls-20170308050001.xml',
        # 'calls-20170309065640.xml',
        'calls-20170310063055.xml',
        # 'calls-20170311050001.xml',
        # 'calls-20170312050001.xml',
        # 'calls-20170313050001.xml',
        # 'calls-20170314051813.xml',
        'calls-20170315050001.xml',

        # 'calls-20210901043042.xml',
        'calls-20210902043044.xml',
        # 'calls-20210903043044.xml',
        # 'calls-20210904060930.xml',
        # 'calls-20210905043030.xml',
        # 'calls-20210906043031.xml',
        'calls-20210907043032.xml',
        'calls-20210908043032.xml',

        'sms-20211008043028.xml',
        # 'sms-20211009043028.xml'
        'sms-20211010043029.xml',
        # 'sms-20211011043029.xml',
        'sms-20211012065557.xml',
        # 'sms-20211013043058.xml',
        # 'sms-20211014043058.xml',
        # 'sms-20211015043059.xml',
        # 'sms-20211016043059.xml',
        # 'sms-20211017043000.xml',
        # 'sms-20211018045758.xml',
        # 'sms-20211019043059.xml',
        # 'sms-20211020043100.xml',
        # 'sms-20211021043000.xml',
        # 'sms-20211022044756.xml',
        # 'sms-20211023043057.xml',
        # 'sms-20211024043057.xml',
        # 'sms-20211025043057.xml',
        # 'sms-20211026051803.xml',
        # 'sms-20211027043004.xml',
        # 'sms-20211028043004.xml',
        'sms-20211029043004.xml',
        # 'sms-20211030043005.xml',
        # 'sms-20211031043005.xml',
        # 'sms-20211101043006.xml',
        # 'sms-20211102043006.xml',
        # 'sms-20211103043007.xml',
        # 'sms-20211104043007.xml',
        # 'sms-20211105102901.xml',
        # 'sms-20211106043002.xml',
        # 'sms-20211107043002.xml',
        # 'sms-20211108043003.xml',
        # 'sms-20211109043004.xml',
        'sms-20211110043004.xml',
    ]  # fmt: skip
