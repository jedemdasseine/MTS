# -*- coding: utf-8 -*-
import common


class DbManager(object):

    # Constructor of class
    def __init__(self, driver, *args, **kwargs):
        self.__driver = driver
        self.__connection = None
        self.__cursor = None
        self.connect(*args, **kwargs)

    # Check active connection or not
    def is_active(self):
        return self.__connection is not None

    # Connect to database
    def connect(self, *args, **kwargs):
        if not self.is_active():
            self.__connection = self.__driver.connect(*args, **kwargs)
            self.__cursor = self.__connection.cursor()

    # Save changes to database
    def commit(self):
        if self.is_active():
            self.__connection.commit()

    # Connect to database
    def __enter__(self):
        return self

    # Close connection to database
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.is_active():
            self.__connection.close()

    # Insert a row of data
    def __execute_routine(self, *args, **kwargs):
        if self.is_active():
            self.__cursor.execute(*args, **kwargs)

    # Insert a few rows of data
    def __result_iterator_routine(self, chunk_size=common.CHUNK_SIZE):
        if not self.is_active():
            return
        while True:
            results = self.__cursor.fetchmany(chunk_size)
            if not results:
                break
            for result in results:
                yield result

    # Method to insert one row of data with exception handling
    def execute(self, *args, **kwargs):
        try:
            self.__execute_routine(*args, **kwargs)
            return True
        except self.__driver.DatabaseError as err:
            print("Error: " + str(err))
            return False

    # Method to insert a few rows of data with exception handling
    def execute_results(self, *args, **kwargs):
        try:
            self.__execute_routine(*args, **kwargs)
            return self.__result_iterator_routine()
        except self.__driver.DatabaseError as err:
            print("Error: " + str(err))
            return []
