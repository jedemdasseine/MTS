# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import sqlite3
from db_api import DbManager
import datetime
import common


# Add additional arguments
def populate_argparser(argparser):
    argparser.add_argument(
        '--db',
        metavar="PATH",
        required=True,
        help="Path to sqlite db"
    )


def main(args):

    # Add variables for output
    equal_id_int = 0
    count = 0
    null_count_float = 0
    zero_count_float = 0
    int_avg = 0
    float_avg = 0

    with DbManager(sqlite3, args.db) as db:

        # Get today's data from the database
        for res in db.execute_results('SELECT * FROM {0} WHERE {1} = "{2}"'.
                                      format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL,
                                             str(datetime.datetime.now().date()))):

            # Parse tuple with data from database
            load_date = res[0]
            _id = res[1]
            int_value = res[2]
            float_value = res[3]
            char_value = res[4]
            date_value = res[5]

            # Calculate non unique combinations, NULL values, zero values
            if load_date is None:
                null_count_float += 1
            if _id == int_value:
                equal_id_int += 1
            if _id == 0:
                zero_count_float += 1
            elif _id is None:
                null_count_float += 1
            if int_value is not None:
                int_avg = int_avg + int_value
            if int_value is None:
                null_count_float += 1
            elif int_value == 0:
                zero_count_float += 1
            if float_value is not None:
                float_avg = float_avg + float_value
            if float_value is None:
                null_count_float += 1
            elif float_value == 0:
                zero_count_float += 1
            if char_value is None:
                null_count_float += 1
            if date_value is None:
                null_count_float += 1

            # Calculate number of rows
            count += 1

    # Calculate the average value
    if count == 0:
        raise ZeroDivisionError('Error: There is no today data in CHECK_OBJECT table')
    else:
        int_avg = int_avg / count
        float_avg = float_avg / count

    with DbManager(sqlite3, args.db) as db:

        # Create db and table
        if not db.execute('CREATE TABLE IF NOT EXISTS {0} ({1} {2}, {3} {4}, {5} {6}, {7} {8}, {9} {10}, {11} {12})'.
                          format(common.CHECK_STATUS_TABLE, common.COT_NON_UNIQUE_COL,
                                 common.COT_NON_UNIQUE_TYPE, common.COT_TOTAL_COUNT_COL,
                                 common.COT_TOTAL_COUNT_TYPE,
                                 common.COT_NULL_COUNT_COL, common.COT_NULL_COUNT_TYPE, common.COT_ZERO_COUNT_COL,
                                 common.COT_ZERO_COUNT_TYPE, common.COT_AVERAGE_INT_COL,
                                 common.COT_AVERAGE_INT_TYPE,
                                 common.COT_AVERAGE_FLOAT_COL, common.COT_AVERAGE_FLOAT_TYPE)):
            raise common.TableNotCreatedException()
        db.commit()

        # Insert parsed data in table
        db.execute('INSERT OR IGNORE INTO {0} ({1}, {2}, {3}, {4}, {5}, {6}) VALUES (?, ?, ?, ?, ?, ?)'.
                   format(common.CHECK_STATUS_TABLE, common.COT_NON_UNIQUE_COL, common.COT_TOTAL_COUNT_COL,
                          common.COT_NULL_COUNT_COL, common.COT_ZERO_COUNT_COL, common.COT_AVERAGE_INT_COL,
                          common.COT_AVERAGE_FLOAT_COL),
                   (equal_id_int, count, null_count_float, zero_count_float, int_avg, float_avg))
        db.commit()


# Insert point
if __name__ == '__main__':
    argparser = ArgumentParser()
    populate_argparser(argparser)
    args = argparser.parse_args()
    main(args)
