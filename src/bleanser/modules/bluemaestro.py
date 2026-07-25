import json

from bleanser.core.modules.sqlite import SqliteNormaliser, Tool


class Normaliser(SqliteNormaliser):
    # multiway is useful at the very least for old db format, it only kept rolling 6K points or something in the db
    MULTIWAY = True
    PRUNE_DOMINATED = True

    def check(self, c) -> None:
        tool = Tool(c)
        tables = tool.get_tables()
        info_tables = [x for x in tables if x.endswith('_info')]
        if len(info_tables) == 0:
            # old db format
            data = tables['data']
            assert 'Time' in data, data
            assert 'Temperature' in data, data
        else:
            # TODO hmm how to add some proper check here without too much duplication?
            pass

    def cleanup(self, c) -> None:
        self.check(c)
        tool = Tool(c)

        tables = tool.get_tables()
        info_tables = [x for x in tables if x.endswith('_info')]
        if len(info_tables) == 0:
            # old db format
            # log_index doesn't correspond to anything real, there are timestamps
            tool.drop_cols(table='data', cols=['log_index'])
            # changes every time db is exported, no point
            tool.drop_cols(table='info', cols=['last_download', 'last_pointer'])
            return

        for info_table in info_tables:
            # possible to have multiple info tables, e.g. if you have multiple devices

            device = info_table.removesuffix('_info')

            ## get rid of downloadUnix -- it's changing after export and redundant info
            # Answer: lastDownloadTableName is the same derived pointer in newer exports.
            [[download_unix, last_download_table]] = list(
                c.execute(f'SELECT downloadUnix, lastDownloadTableName FROM {info_table}')
            )
            download_table = f'{device}_{download_unix}_log'
            if download_table in tables:
                # TODO annoying that it needs to be defensive...
                # for some dbs it actually does happen, e.g. around 20211102085345
                # Answer: The per-device existence check preserves those incomplete exports.
                assert last_download_table in {None, '', download_table}, (
                    info_table,
                    last_download_table,
                    download_table,
                )
                # Both columns point to the retained download table and otherwise change on each export.
                tool.drop_cols(
                    table=info_table,
                    cols=['downloadUnix', 'lastDownloadTableName'],
                )

        # Device stubs mix transient state with settings such as the logging interval, alerts, and calibrations,
        #   so remove only the volatile fields.
        volatile_stub_keys = {
            'rssi',  # Latest Bluetooth signal strength.
            'battery',  # Latest advertised battery level.
            'logCount',  # Current log count advertised by the device.
            'lastDetected',  # Latest Bluetooth detection timestamp.
            'lastDownloadedUnix',  # Latest download timestamp.
            'totalLogsSavedOnPhone',  # Phone-local saved-record count.
        }
        for stub_table in [table for table in tables if table.endswith('_stub')]:
            for rowid, stub_json in c.execute(f'SELECT rowid, deviceStub FROM `{stub_table}`'):
                stub = json.loads(stub_json)
                assert isinstance(stub, list), stub
                assert len(stub) >= 3, stub
                device_state = stub[0]
                assert isinstance(device_state, dict), device_state
                for key in volatile_stub_keys:
                    device_state.pop(key, None)
                stub.pop(2)  # Raw BLE manufacturer data is transient connection state.
                c.execute(
                    f'UPDATE `{stub_table}` SET deviceStub = ? WHERE rowid = ?',
                    (json.dumps(stub, ensure_ascii=False, separators=(',', ':')), rowid),
                )

        # An omnibus table is the app's rolling aggregate of a device's measurements.
        # Its measurements are duplicated in the timestamped per-download log tables.
        # However, just to be conservative and avoid data loss, we want to validate that it indeed contains them before dropping it.
        measurement_columns = ('unix', 'tempReadings', 'humiReadings', 'pressReadings', 'dewpReadings')
        for omnibus_table in [table for table in tables if table.endswith('_omnibus_log')]:
            device = omnibus_table.removesuffix('_omnibus_log')
            log_tables = [
                table
                for table, schema in tables.items()
                if table.startswith(f'{device}_')
                and table.endswith('_log')
                and table != omnibus_table
                and set(measurement_columns) <= schema.keys()
            ]
            if len(log_tables) == 0:
                continue

            columns_sql = ', '.join(measurement_columns)
            normal_data_sql = ' UNION ALL '.join(f'SELECT {columns_sql} FROM `{log_table}`' for log_table in log_tables)
            # This safety check took about 0.4 seconds, or 10% of a full normaliser pass, on a 60 MB export.
            # Keep the cache if it contains even one measurement absent from the per-download logs.
            extra_row = c.execute(f'''
                WITH normal_data AS ({normal_data_sql})
                SELECT {columns_sql} FROM `{omnibus_table}`
                EXCEPT
                SELECT * FROM normal_data
                LIMIT 1
            ''').fetchone()
            if extra_row is not None:
                continue
            tool.drop(omnibus_table)  # Rolling cache duplicates retained per-download logs.


if __name__ == '__main__':
    Normaliser.main()


def test_new_format_cleanup() -> None:
    import sqlite3

    with sqlite3.connect(':memory:') as c:
        c.executescript('''
            CREATE TABLE A_info(downloadUnix INTEGER, lastDownloadTableName TEXT);
            INSERT INTO A_info VALUES(1, 'A_1_log');
            CREATE TABLE A_1_log(
                id INTEGER,
                unix INTEGER,
                tempReadings INTEGER,
                humiReadings INTEGER,
                pressReadings INTEGER,
                dewpReadings INTEGER
            );
            INSERT INTO A_1_log VALUES(0, 1, 2, 3, 4, 5);
            CREATE TABLE A_omnibus_log(
                id INTEGER,
                unix INTEGER,
                tempReadings INTEGER,
                humiReadings INTEGER,
                pressReadings INTEGER,
                dewpReadings INTEGER
            );
            INSERT INTO A_omnibus_log VALUES(10, 1, 2, 3, 4, 5);

            CREATE TABLE B_info(downloadUnix INTEGER, lastDownloadTableName TEXT);
            INSERT INTO B_info VALUES(2, 'B_2_log');
            CREATE TABLE B_2_log(
                id INTEGER,
                unix INTEGER,
                tempReadings INTEGER,
                humiReadings INTEGER,
                pressReadings INTEGER,
                dewpReadings INTEGER
            );
            INSERT INTO B_2_log VALUES(0, 2, 3, 4, 5, 6);
            CREATE TABLE B_omnibus_log(
                id INTEGER,
                unix INTEGER,
                tempReadings INTEGER,
                humiReadings INTEGER,
                pressReadings INTEGER,
                dewpReadings INTEGER
            );
            INSERT INTO B_omnibus_log VALUES(10, 20, 30, 40, 50, 60);

            CREATE TABLE C_info(downloadUnix INTEGER, lastDownloadTableName TEXT);
            INSERT INTO C_info VALUES(3, 'C_3_log');
        ''')
        stub = [
            {
                'battery': 90,
                'interval': 60,
                'lastDetected': 123,
                'rssi': -50,
            },
            'A',
            {'raw': 'manufacturer data'},
            'preserved',
        ]
        c.execute('CREATE TABLE A_stub(deviceStub TEXT)')
        c.execute('INSERT INTO A_stub VALUES(?)', (json.dumps(stub),))

        normaliser = object.__new__(Normaliser)
        normaliser.cleanup(c)

        assert list(c.execute('SELECT downloadUnix, lastDownloadTableName FROM A_info')) == [(None, None)]
        assert list(c.execute('SELECT downloadUnix, lastDownloadTableName FROM B_info')) == [(None, None)]
        assert list(c.execute('SELECT downloadUnix, lastDownloadTableName FROM C_info')) == [(3, 'C_3_log')]

        tables = Tool(c).get_tables()
        assert 'A_omnibus_log' not in tables
        assert 'B_omnibus_log' in tables

        [[stub_json]] = c.execute('SELECT deviceStub FROM A_stub')
        cleaned_stub = json.loads(stub_json)
        assert cleaned_stub == [{'interval': 60}, 'A', 'preserved']


# TODO think I've had jdoe or something with example databases..
def test_bluemaestro() -> None:
    from bleanser.tests.common import skip_if_no_data

    skip_if_no_data()

    from bleanser.tests.common import TESTDATA, actions2

    res = actions2(path=TESTDATA / 'bluemaestro', rglob='**/*.db*', Normaliser=Normaliser)

    assert res.remaining == [
        '20180720.db',
        # '20180724.db',  # move
        '20180728.db',
        # '20180730.db',  # move
        '20180731.db',

        '20190723100032.db',  # keep, everything changed
        # TODO need to investigate, some values have changed a bit, like 1st digit after decimal point
        # even timestamps changed sometimes (e.g. just last second)
        # hpi bluemaestro module has something for handling this, I think
        '20190724101707.db',
        # same as above
        '20190727104723.db',

        '20200208225936.db',  # keep, everything changed (several months diff)
        # '20201209083427/bmgateway.db',  # move, completely dominated by the next
        # '20210131102917/bmgateway.db',  # move, completely dominated by the next
        # '20210207183947/bmgateway.db',  # move, completely dominated by the next
        '20210216211844/bmgateway.db',  # keep, errored because couldn't find last _log item
        '20211103234924/bmgateway.db',  # same, previous errored
        '20211106191208/bmgateway.db',
    ]  # fmt: skip
