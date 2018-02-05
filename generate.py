# -*- coding: utf-8 -*-

import datetime
import random
import sqlite3
import string
import common
from db_api import DbManager
from argparse import ArgumentParser


def populate_argparser(argparser):
    """
    Add additional arguments

    :param argparser:
    """

    # Add "count" argument
    argparser.add_argument(
        '--count',
        metavar="UINT",
        required=True,
        type=int,
        help="Number of rows in table",
    )

    # Add "output" argument
    argparser.add_argument(
        '--output',
        metavar="PATH",
        required=True,
        help="Path to output sqlite db"
    )


def create_db(db):
    """
    Create db and table

    :param db: DbManager object
    """
    if not db.execute('CREATE TABLE IF NOT EXISTS {0} ({1} {2}, {3} {4}, {5} {6}, {7} {8}, {9} {10}, {11} {12})'.
                      format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL, common.COT_LOAD_DATE_TYPE,
                             common.COT_ID_COL, common.COT_ID_TYPE, common.COT_INT_COL, common.COT_INT_TYPE,
                             common.COT_FLOAT_COL, common.COT_FLOAT_TYPE, common.COT_CHAR_COL, common.COT_CHAR_TYPE,
                             common.COT_DATE_COL, common.COT_DATE_TYPE)):
        raise common.TableNotCreatedException()
    db.commit()


def generate_datelist(now):
    """
    Generate date list

    :param now: Today's date
    :return: List with dates
    """
    dates = []
    for i in range(0, common.DEFAULT_DAYS):
        dates.append(now - datetime.timedelta(days=i))
    return dates


def randomize_int_number(start_value, finish_value):
    """
    Randomize int number in range

    :param start_value: Start value of range
    :param finish_value: Finish value of range
    :return: Random int number
    """
    return random.randint(start_value, finish_value)


def randomize_float_number():
    """
    Randomize float number

    :return: Random float number
    """
    return random.random()


def generate_load_date(dates, rand):
    """
    Generate load_date

    :param dates: List with dates
    :param rand: Random int number
    :return: LOAD_DATE value
    """
    if rand == len(dates):
        load_date = None
    else:
        load_date = dates[rand]
    return load_date


def generate_id(rand):
    """
    Generate data for ID

    :param rand: Random int number
    :return: ID value
    """
    if rand < 100:
        _id = None
    else:
        _id = rand
    return _id


def generate_int_value(_id, rand):
    """
    Generate data for INT_VALUE

    :param _id: ID value
    :param rand: Random int number
    :return: INT_VALUE value
    """
    if rand == 0:
        int_value = rand
    elif rand < 100:
        int_value = None
    elif rand < 200:
        int_value = _id
    else:
        int_value = rand
    return int_value


def generate_float_value(rand):
    """
    Generate data for FLOAT VALUE
    
    :param rand: Random float number
    :return: FLOAT_VALUE value
    """
    if rand < 0.05:
        float_value = None
    else:
        float_value = rand
    return float_value


def generate_char_value(rand):
    """
    Generate data for CHAR_VALUE

    :param rand: Random int number
    :return: CHAR_VALUE value
    """
    if rand == -1:
        char_value = None
    else:
        char_value = ''.join(random.choice(string.ascii_uppercase) for _ in range(rand))
    return char_value


def generate_date_value(dates, rand):
    """
    Generate data for DATE_VALUE

    :param dates: List with dates
    :param rand: Random int number
    :return: DATE_VALUE value
    """
    if rand == len(dates):
        date_value = None
    else:
        date_value = dates[rand]
    return date_value


def insert_data_in_db(i, db, load_date, _id, int_value, float_value, char_value, date_value):
    """
    Insert generated data in database

    :param i: iterator value
    :param db: DbManager object
    :param load_date: LOAD_DATE value
    :param _id: ID value
    :param int_value: INT_VALUE value
    :param float_value: FLOAT_VALUE value
    :param char_value: CHAR_VALUE value
    :param date_value: DATE_VALUE value
    """

    # Starting new SQL transaction after some rows
    if (i % common.TRANSACTION_SIZE) == 0:
        db.commit()
        db.execute('BEGIN TRANSACTION')

    # Insert generated data in table
    db.execute('INSERT OR IGNORE INTO {0} ({1}, {2}, {3}, {4}, {5}, {6}) VALUES (?, ?, ?, ?, ?, ?)'.
               format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL, common.COT_ID_COL,
                      common.COT_INT_COL, common.COT_FLOAT_COL, common.COT_CHAR_COL,
                      common.COT_DATE_COL),
               (load_date, _id, int_value, float_value, char_value, date_value))


def generate_data_and_insert_in_db(args, dates, db):
    """
    Generate data for database

    :param args: Arguments from command string
    :param dates: List with dates
    :param db: DbManager object
    """
    for i in range(0, args.count):
        load_date = generate_load_date(dates, randomize_int_number(0, len(dates)))
        _id = generate_id(randomize_int_number(-1, 10000))
        int_value = generate_int_value(_id, randomize_int_number(-1, 10000))
        float_value = generate_float_value(randomize_float_number())
        char_value = generate_char_value(randomize_int_number(-1, 10))
        date_value = generate_date_value(dates, randomize_int_number(0, len(dates)))
        insert_data_in_db(i, db, load_date, _id, int_value, float_value, char_value, date_value)
    db.commit()


def main(args):
    """
    Main function

    :param args: Arguments from command string
    """
    with DbManager(sqlite3, args.output) as db:
        create_db(db)
        generate_data_and_insert_in_db(args, generate_datelist(datetime.datetime.now().date()), db)


# Insert point
if __name__ == '__main__':
    argparser = ArgumentParser()
    populate_argparser(argparser)
    args = argparser.parse_args()
    main(args)
