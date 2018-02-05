# -*- coding: utf-8 -*-

import datetime
import sqlite3
from argparse import ArgumentParser

import common
from db_api import DbManager


def populate_argparser(argparser):
    """
    Add additional arguments

    :param argparser: argparser object
    """
    argparser.add_argument(
        '--db',
        metavar="PATH",
        required=True,
        help="Path to sqlite db"
    )


def calculate_null_count_float(null_count_float, load_date, _id, int_value, float_value, char_value, date_value):
    """
    Calculate count of NULL values

    :param null_count_float: Old count
    :param load_date: LOAD_DATE value
    :param _id: ID value
    :param int_value: INT_VALUE value
    :param float_value: FLOAT_VALUE value
    :param char_value: CHAR_VALUE value
    :param date_value: DATE_VALUE value
    :return: New count
    """
    if load_date is None:
        null_count_float += 1
    if _id is None:
        null_count_float += 1
    if int_value is None:
        null_count_float += 1
    if float_value is None:
        null_count_float += 1
    if char_value is None:
        null_count_float += 1
    if date_value is None:
        null_count_float += 1
    return null_count_float


def calculate_zero_count_float(zero_count_float, _id, int_value, float_value):
    """
    Calculate count of zero values

    :param zero_count_float: Old count
    :param _id: ID value
    :param int_value: INT_VALUE value
    :param float_value: FLOAT_VALUE value
    :return: New count
    """
    if _id == 0:
        zero_count_float += 1
    elif int_value == 0:
        zero_count_float += 1
    elif float_value == 0:
        zero_count_float += 1
    return zero_count_float


def calculate_equal_id_int(equal_id_int, _id, int_value):
    """
    Calculate count of rows, where ID value = INT_VALUE value
    :param equal_id_int: Old count of rows
    :param _id: ID value
    :param int_value: INT_VALUE value
    :return: New count of rows
    """
    if _id == int_value:
        equal_id_int += 1
    return equal_id_int


def calculate_sum_of_int_values(int_sum, int_value):
    """
    Calculate sum of INT_VALUEs
    :param int_sum: Old sum of INT_VALUEs
    :param int_value: : New INT_VALUE
    :return: New sum of INT_VALUEs
    """
    if int_value is not None:
        int_sum = int_sum + int_value
    return int_sum


def calculate_sum_of_float_values(float_sum, float_value):
    """
    Calculate sum of FLOAT_VALUEs

    :param float_sum: Old sum of FLOAT_VALUEs
    :param float_value: New FLOAT_VALUE
    :return: New sum of FLOAT_VALUEs
    """
    if float_value is not None:
        float_sum = float_sum + float_value
    return float_sum


def calculate_count(count):
    """
    Calculate count of rows

    :param count: Count of rows
    :return: Count of rows
    """
    count += 1
    return count


def calculate_average_int(count, sum_int):
    """
    Calculate average INT_VALUE

    :param count: Count of INT_VALUE values
    :param sum_int: Sum of INT_VALUE values
    :return: Average INT_VALUE
    """
    if count == 0:
        raise ZeroDivisionError('Error: There is no today data in CHECK_OBJECT table')
    else:
        sum_int = sum_int / count
    return sum_int


def calculate_average_float(count, sum_float):
    """
    Calculate average FLOAT_VALUE
    
    :param count: Count of FLOAT_VALUE values
    :param sum_float: Sum of FLOAT_VALUE values
    :return: Average FLOAT_VALUE
    """
    if count == 0:
        raise ZeroDivisionError('Error: There is no today data in CHECK_OBJECT table')
    else:
        sum_float = sum_float / count
    return sum_float


def create_result_table(db):
    """
    Create db and table

    :param db: DbManager object
    """
    if not db.execute('CREATE TABLE IF NOT EXISTS {0} ({1} {2}, {3} {4}, {5} {6}, {7} {8}, {9} {10}, {11} {12})'.
                      format(common.CHECK_STATUS_TABLE, common.COT_NON_UNIQUE_COL, common.COT_NON_UNIQUE_TYPE,
                             common.COT_TOTAL_COUNT_COL, common.COT_TOTAL_COUNT_TYPE, common.COT_NULL_COUNT_COL,
                             common.COT_NULL_COUNT_TYPE, common.COT_ZERO_COUNT_COL, common.COT_ZERO_COUNT_TYPE,
                             common.COT_AVERAGE_INT_COL, common.COT_AVERAGE_INT_TYPE,
                             common.COT_AVERAGE_FLOAT_COL, common.COT_AVERAGE_FLOAT_TYPE)):
        raise common.TableNotCreatedException()
    db.commit()


def get_and_parse_data_from_db(db):
    """
    Get today's data and parse it

    :param db: DbManager object
    :return: List with parsed data
    """

    # Add variables for output
    equal_id_int = 0
    count = 0
    null_count_float = 0
    zero_count_float = 0
    int_sum = 0
    float_sum = 0

    # Get today's data from the database
    for res in db.execute_results('SELECT * FROM {0} WHERE {1} = "{2}"'.
                                  format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL,
                                         str(datetime.datetime.now().date()))):
        # Parse today's data
        null_count_float = calculate_null_count_float(null_count_float, res[0], res[1], res[2], res[3], res[4], res[5])
        zero_count_float = calculate_zero_count_float(zero_count_float, res[1], res[2], res[3])
        equal_id_int = calculate_equal_id_int(equal_id_int, res[1], res[2])
        int_sum = calculate_sum_of_int_values(int_sum, res[2])
        float_sum = calculate_sum_of_float_values(float_sum, res[3])
        count = calculate_count(count)
    return [equal_id_int, count, null_count_float, zero_count_float, int_sum, float_sum]


def insert_data_in_result_db(db, result_data_list):
    """
    Insert parsed data in table

    :param db: DbManager object
    :param result_data_list: List with parsed data
    """
    db.execute('INSERT OR IGNORE INTO {0} ({1}, {2}, {3}, {4}, {5}, {6}) VALUES (?, ?, ?, ?, ?, ?)'.
               format(common.CHECK_STATUS_TABLE, common.COT_NON_UNIQUE_COL, common.COT_TOTAL_COUNT_COL,
                      common.COT_NULL_COUNT_COL, common.COT_ZERO_COUNT_COL, common.COT_AVERAGE_INT_COL,
                      common.COT_AVERAGE_FLOAT_COL),
               (result_data_list[0], result_data_list[1], result_data_list[2], result_data_list[3],
                calculate_average_int(result_data_list[1], result_data_list[4]),
                calculate_average_float(result_data_list[1], result_data_list[5])))
    db.commit()


def main(args):
    """
    Main function

    :param args: Arguments from command string
    """
    with DbManager(sqlite3, args.db) as db:
        result_data_list = get_and_parse_data_from_db(db)
        create_result_table(db)
        insert_data_in_result_db(db, result_data_list)


# Insert point
if __name__ == '__main__':
    argparser = ArgumentParser()
    populate_argparser(argparser)
    args = argparser.parse_args()
    main(args)
