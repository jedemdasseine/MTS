import generate
import calc_stats
import datetime
import common
from db_api import DbManager
import sqlite3


class TestCheckStatusCreate:

    def test_check_status_create(self):

        with DbManager(sqlite3, "MTSDB.db") as db:
            calc_stats.create_result_table(db)
            assert (db.execute("SELECT * FROM {0}".format(common.CHECK_STATUS_TABLE)))

    def teardown(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            db.execute("DROP TABLE {0}".format(common.CHECK_STATUS_TABLE))


class TestCheckStatusResultsInsert:

    result_list = [3, 100, 5, 3, 500, 0.5]

    def setup(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            db.execute('CREATE TABLE IF NOT EXISTS {0} ({1} {2}, {3} {4}, {5} {6}, {7} {8}, {9} {10}, {11} {12})'.
                       format(common.CHECK_STATUS_TABLE, common.COT_NON_UNIQUE_COL, common.COT_NON_UNIQUE_TYPE,
                              common.COT_TOTAL_COUNT_COL, common.COT_TOTAL_COUNT_TYPE, common.COT_NULL_COUNT_COL,
                              common.COT_NULL_COUNT_TYPE, common.COT_ZERO_COUNT_COL, common.COT_ZERO_COUNT_TYPE,
                              common.COT_AVERAGE_INT_COL, common.COT_AVERAGE_INT_TYPE,
                              common.COT_AVERAGE_FLOAT_COL, common.COT_AVERAGE_FLOAT_TYPE))

    def test_check_status_results_insert(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            calc_stats.insert_data_in_result_db(db, self.result_list)
            for av in db.execute_results("SELECT * FROM {0}".format(common.CHECK_STATUS_TABLE)):
                assert (av == tuple(self.result_list))

    def teardown(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            db.execute("DROP TABLE {0}".format(common.CHECK_STATUS_TABLE))


class TestCheckObjectCreate:

    def test_check_status_create(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            generate.create_table(db)
            assert (db.execute("SELECT * FROM {0}".format(common.CHECK_OBJECT_TABLE)))

    def teardown(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            db.execute("DROP TABLE {0}".format(common.CHECK_OBJECT_TABLE))


class TestCheckObjectDataInsert:

    result_list = [datetime.date(2018, 2, 5), 10, 45, 0.5, "SNHFY", datetime.date(2018, 2, 3)]

    def setup(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            db.execute('CREATE TABLE IF NOT EXISTS {0} ({1} {2}, {3} {4}, {5} {6}, {7} {8}, {9} {10}, {11} {12})'.
                       format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL, common.COT_LOAD_DATE_TYPE,
                              common.COT_ID_COL, common.COT_ID_TYPE, common.COT_INT_COL, common.COT_INT_TYPE,
                              common.COT_FLOAT_COL, common.COT_FLOAT_TYPE, common.COT_CHAR_COL, common.COT_CHAR_TYPE,
                              common.COT_DATE_COL, common.COT_DATE_TYPE))

    def test_check_object_data_insert(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            generate.insert_data_in_db(0, db, self.result_list)
            self.result_list[0] = str(self.result_list[0])
            self.result_list[5] = str(self.result_list[5])
            for av in db.execute_results("SELECT * FROM {0}".format(common.CHECK_OBJECT_TABLE)):
                assert (av == tuple(self.result_list))

    def teardown(self):
        with DbManager(sqlite3, "MTSDB.db") as db:
            db.execute("DROP TABLE {0}".format(common.CHECK_OBJECT_TABLE))
