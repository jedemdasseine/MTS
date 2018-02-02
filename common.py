# -*- coding: utf-8 -*-

# CHECK_OBJECT table constants
CHECK_OBJECT_TABLE = "CHECK_OBJECT"
COT_LOAD_DATE_COL = "LOAD_DATE"
COT_LOAD_DATE_TYPE = "DATE"
COT_ID_COL = "ID"
COT_ID_TYPE = "INTEGER"
COT_INT_COL = "INT_VALUE"
COT_INT_TYPE = "INTEGER"
COT_FLOAT_COL = "FLOAT_VALUE"
COT_FLOAT_TYPE = "FLOAT"
COT_CHAR_COL = "CHAR_VALUE"
COT_CHAR_TYPE = "VARCHAR(10)"
COT_DATE_COL = "DATE_VALUE"
COT_DATE_TYPE = "DATE"

# CHECK_STATUS table constants
CHECK_STATUS_TABLE = "CHECK_STATUS"
COT_NON_UNIQUE_COL = "NON_UNIQUE"
COT_NON_UNIQUE_TYPE = "INTEGER"
COT_TOTAL_COUNT_COL = "TOTAL_COUNT"
COT_TOTAL_COUNT_TYPE = "INTEGER"
COT_NULL_COUNT_COL = "NULL_COUNT"
COT_NULL_COUNT_TYPE = "INTEGER"
COT_ZERO_COUNT_COL = "ZERO_COUNT"
COT_ZERO_COUNT_TYPE = "INTEGER"
COT_AVERAGE_INT_COL = "AVERAGE_INT"
COT_AVERAGE_INT_TYPE = "FLOAT"
COT_AVERAGE_FLOAT_COL = "AVERAGE_FLOAT"
COT_AVERAGE_FLOAT_TYPE = "FLOAT"

# Other constants
DEFAULT_DAYS = 5
TRANSACTION_SIZE = 10000
CHUNK_SIZE = 10000


# Exception
class TableNotCreatedException(Exception):
    pass
