# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import random
import sqlite3
from db_api import DbManager
import datetime
import common
import string


# Add additional arguments
def populate_argparser(argparser):
    argparser.add_argument(
        '--count',
        metavar="UINT",
        required=True,
        type=int,
        help="Number of rows in table",
    )
    argparser.add_argument(
        '--output',
        metavar="PATH",
        required=True,
        help="Path to output sqlite db"
    )


# Generate date list
def generate_datelist(now):
    dates = []
    for i in range(0, common.DEFAULT_DAYS):
        dates.append(now - datetime.timedelta(days=i))
    return dates


# Generate load_date
def generate_load_date(dates):
    j = random.randint(0, len(dates))
    if j == len(dates):
        load_date = None
    else:
        load_date = dates[j]
    return load_date


# Generate data for ID
def generate_id():
    j = random.randint(-1, 10000)
    if j < 100:
        _id = None
    else:
        _id = j
    return _id


# Generate data for INT_VALUE
def generate_int_value(_id):
    j = random.randint(-1, 10000)
    if j == 0:
        int_value = j
    elif j < 100:
        int_value = None
    elif j < 200:
        int_value = _id
    else:
        int_value = j
    return int_value


# Generate data for FLOAT VALUE
def generate_float_value():
    j = random.random()
    if j < 0.05:
        float_value = None
    else:
        float_value = j
    return float_value


# Generate data for CHAR_VALUE
def generate_char_value():
    j = random.randint(-1, 10)
    if j == -1:
        char_value = None
    else:
        char_value = ''.join(random.choice(string.ascii_uppercase) for _ in range(j))
    return char_value


# Generate data for DATE_VALUE
def generate_date_value(dates):
    j = random.randint(0, len(dates))
    if j == len(dates):
        date_value = None
    else:
        date_value = dates[j]
    return date_value


def insert_data_in_db(i, db, load_date, _id, int_value, float_value, char_value, date_value):

    #Starting new SQL transaction after some rows
    if (i % common.TRANSACTION_SIZE) == 0:
        db.commit()
        db.execute('BEGIN TRANSACTION')

    # Insert generated data in table
    db.execute('INSERT OR IGNORE INTO {0} ({1}, {2}, {3}, {4}, {5}, {6}) VALUES (?, ?, ?, ?, ?, ?)'.
               format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL, common.COT_ID_COL,
                      common.COT_INT_COL, common.COT_FLOAT_COL, common.COT_CHAR_COL,
                      common.COT_DATE_COL),
               (load_date, _id, int_value, float_value, char_value, date_value))


# Generate data for database
def generate_data_for_db(args, dates, db):
    for i in range(0, args.count):
        _id = generate_id()
        insert_data_in_db(i, db, generate_load_date(dates), _id, generate_int_value(_id),
                          generate_float_value(), generate_char_value(), generate_date_value(dates))
    db.commit()


def main(args):
    with DbManager(sqlite3, args.output) as db:

        # Create db and table
        if not db.execute('CREATE TABLE IF NOT EXISTS {0} ({1} {2}, {3} {4}, {5} {6}, {7} {8}, {9} {10}, {11} {12})'.
                          format(common.CHECK_OBJECT_TABLE, common.COT_LOAD_DATE_COL,
                                 common.COT_LOAD_DATE_TYPE, common.COT_ID_COL, common.COT_ID_TYPE,
                                 common.COT_INT_COL, common.COT_INT_TYPE, common.COT_FLOAT_COL,
                                 common.COT_FLOAT_TYPE, common.COT_CHAR_COL, common.COT_CHAR_TYPE,
                                 common.COT_DATE_COL, common.COT_DATE_TYPE)):
            raise common.TableNotCreatedException()
        db.commit()
        datelist = generate_datelist(datetime.datetime.now().date())
        generate_data_for_db(args, datelist, db)


# Insert point
if __name__ == '__main__':
    argparser = ArgumentParser()
    populate_argparser(argparser)
    args = argparser.parse_args()
    main(args)
