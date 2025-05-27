import json

from bleanser.core.modules.json import delkeys
from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tables = Tool(c).get_tables()
        messages = tables['conversationsv14']
        assert 'nsp_data' in messages, messages

    def cleanup(self, c) -> None:
        self.check(c)

        t = Tool(c)
        t.drop('conversationsv14_searchTerms_content')
        t.drop('conversationsv14_searchTerms_segments')
        t.drop('conversationsv14_searchTerms_segdir')

        t.drop('internaldata')  # very volatile

        t.drop('telemetrycachev3')  # volatile, nothing interesting here

        def _cleanup_jsons(s):
            if s is None:
                return None
            j = json.loads(s)
            delkeys(
                j,
                keys=[
                    'fetchedDate',  # from profilecachev8, very volatile
                    'up',  # from miniprofilecachev8, very volatile
                ],
            )
            return json.dumps(j)

        c.create_function("CLEANUP_JSONS", 1, _cleanup_jsons)
        list(c.execute('UPDATE profilecachev8     SET nsp_data = CLEANUP_JSONS(nsp_data)'))
        list(c.execute('UPDATE miniprofilecachev8 SET nsp_data = CLEANUP_JSONS(nsp_data)'))


if __name__ == '__main__':
    Normaliser.main()
