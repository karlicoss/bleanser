#!/usr/bin/env python3
from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # events are only snapshots, so probs makes sense
    MULTIWAY = True
    PRUNE_DOMINATED = True


    def check(self, c) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        assert 'content'     in tables, tables
        bm = tables['Bookmark']
        assert 'ExtraAnnotationData' in bm, bm
        assert 'BookmarkID'  in bm, bm
        assert 'DateCreated' in bm, bm
        assert 'BookAuthors' in tables, tables


    def cleanup(self, c) -> None:
        self.check(c)

        tool = Tool(c)

        tool.fix_bad_blob_column(table='Activity', column='Data')
        tool.fix_bad_blob_column(table='Event', column='ExtraData')
        tool.fix_bad_blob_column(table='Bookmark', column='ExtraAnnotationData')
        tool.fix_bad_blob_column(table='user', column='Loyalty')
        tool.fix_bad_blob_column(table='user', column='PrivacyPermissions')

        tool.drop('content') # some cached book data? so not very interesting when it changes..
        tool.drop('content_keys')  # just some image meta
        tool.drop('volume_shortcovers')  # just some hashes
        tool.drop('volume_tabs')  # some hashes

        tool.drop_cols(table='Event', cols=['Checksum'])

        # pointless, they are changing all the time
        c.execute('UPDATE Activity SET Date = NULL WHERE Id = "SomeFakeRecommendedTabId"')
        # TODO not sure about RecentBook?
        c.execute('UPDATE Activity SET Date = NULL WHERE Type IN ("TopPicksTab", "Top50")')
        c.execute('UPDATE Shelf SET _SyncTime = NULL, LastAccessed = NULL, LastModified = NULL WHERE Id = "ReadingList"')
        tool.drop_cols(table='user', cols=['SyncContinuationToken', 'KoboAccessToken', 'KoboAccessTokenExpiry', 'AuthToken', 'RefreshToken'])
        tool.drop_cols(table='Bookmark', cols=[
            'SyncTime',
            'Version', # not sure what it is, but sometimes changing?
            'StartContainerChildIndex', 'EndContainerChildIndex', # ????

            'StartContainerPath', 'EndContainerPath',
        ])
        # TODO ugh. Bookmark.DateCreated sometimes rounds to nearest second? wtf...
        # TODO Event table -- not sure... it trackes event counts, so needs to be cumulative or something?
        # yep, they def seem to messing up a lot
        # TODO Activity -- dates changing all the time... not sure


if __name__ == '__main__':
    Normaliser.main()
