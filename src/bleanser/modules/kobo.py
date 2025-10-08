from sqlite3 import Connection

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # events are only snapshots, so probs makes sense
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c: Connection) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        assert 'content' in tables, tables
        bm = tables['Bookmark']
        # fmt: off
        assert 'ExtraAnnotationData' in bm, bm
        assert 'BookmarkID'          in bm, bm
        assert 'DateCreated'         in bm, bm
        # fmt: on
        assert 'BookAuthors' in tables, tables

    def cleanup(self, c: Connection) -> None:
        self.check(c)

        tool = Tool(c)

        tool.fix_bad_blob_column(table='Activity', column='Data')
        tool.fix_bad_blob_column(table='Event', column='ExtraData')
        tool.fix_bad_blob_column(table='Bookmark', column='ExtraAnnotationData')

        tool.drop('content')  # some cached book data? so not very interesting when it changes..
        tool.drop('content_keys')  # just some image meta
        tool.drop('volume_shortcovers')  # just some hashes
        tool.drop('volume_tabs')  # some hashes
        tool.drop('KoboPlusAssets')  # some builtin faqs/manuals etc
        tool.drop('KoboPlusAssetGroup')  # some builtin faqs/manuals etc
        tool.drop('Tab')  # shop tabs
        tool.drop('Achievement')
        # TODO DbVersion?
        # TODO version in user table?

        tool.drop_cols(table='Event', cols=['Checksum'])

        ## these are changing all the time
        # TODO not sure about RecentBook?
        c.execute('''
        DELETE FROM Activity
        WHERE Type IN (
          "Recommendations",
          "TopPicksTab",
          "Top50"
        )
        ''')
        ##
        # TODO hmm maybe drop all RecentBook from Activity? although doesn't help all that much

        c.execute('''
        DELETE FROM AnalyticsEvents
        WHERE Type IN (
          "PluggedIn",
          "BatteryLevelAtSync"
        )''')

        ## this changes all the time (Shelf only contains some meta entries, this isn't actual book access time)
        c.execute(
            'UPDATE Shelf SET _SyncTime = NULL, LastAccessed = NULL, LastModified = NULL WHERE Id = "ReadingList"'
        )
        ##

        tool.drop_cols(
            table='user',
            cols=[
                'SyncContinuationToken',
                'KoboAccessToken',
                'KoboAccessTokenExpiry',
                'AuthToken',
                'RefreshToken',
                'Loyalty',
                'PrivacyPermissions',  # not very interesting, contains this stuff https://github.com/shadow81627/scrapey/blob/6dc2a7bba7f5adf2e3335c68e30208c71cfb5c2d/cookies.json#L950
            ],
        )
        tool.drop_cols(
            table='Bookmark',
            cols=[
                # TODO UserID??
                # TODO ugh. DateCreated sometimes rounds to nearest second? wtf...
                #
                'SyncTime',
                'Version',  # not sure what it is, but sometimes changing?
                #
                'StartContainerChildIndex',
                'EndContainerChildIndex',  # ????
                #
                'StartContainerPath',
                'EndContainerPath',
            ],
        )
        # TODO Event table -- not sure... it trackes event counts, so needs to be cumulative or something?
        # yep, they def seem to messing up a lot
        # TODO Activity -- dates changing all the time... not sure


if __name__ == '__main__':
    Normaliser.main()
