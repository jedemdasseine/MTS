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


def main(args):

    # Generate date list
    now = datetime.datetime.now().date()
    dates = []
    for i in range(0, common.DEFAULT_DAYS):
        dates.append(now - datetime.timedelta(days=i))

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

        # Generate data for database
        for i in range(0, args.count):

            # Generate load_date
            j = random.randint(0, len(dates))
            if j == len(dates):
                load_date = None
            else:
                load_date = dates[j]

            # Generate data for ID
            j = random.randint(-1, 10000)
            if j < 100:
                _id = None
            else:
                _id = j

            # Generate data for INT_VALUE
            j = random.randint(-1, 10000)
            if j == 0:
                int_value = j
            elif j < 100:
                int_value = None
            elif j < 200:
                int_value = _id
            else:
                int_value = j

            # Generate data for FLOAT VALUE
            j = random.random()
            if j < 0.05:
                float_value = None
            else:
                float_value = j

            # Generate data for CHAR_VALUE
            j = random.randint(-1, 10)
            if j == -1:
                char_value = None
            else:
                char_value = ''.join(random.choice(string.ascii_uppercase) for _ in range(j))

            # Generate data for DATE_VALUE
            j = random.randint(0, len(dates))
            if j == len(dates):
                date_value = None
            else:
                date_value = dates[j]

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
        db.commit()


# Insert point
if __name__ == '__main__':
    argparser = ArgumentParser()
    populate_argparser(argparser)
    args = argparser.parse_args()
    main(args)
