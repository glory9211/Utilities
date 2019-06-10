"""
Class demonstrating the usage of Connector.py
"""

from Connector import ConnectorUtil
import logging

# Logger setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

formater = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('Example_test.log')
file_handler.setFormatter(formater)

logger.addHandler(file_handler)

# Logger setup ended


class OPsql():
    """
    Consits database operation funcitons such as:d
    clean_output --> Clean the returned query from the table
    execute_db_query --> Executes the given query
    """

    def __init__(self):
        self.wire = ConnectorUtil()
        # May create the table in the initializer or not
        # self.create_table()

    def clean_output(self, query, cursor):
        """Clean the returned query from the table"""

        cleaned_result = []
        if 'SELECT' in query:
            for d in cursor.fetchall():
                fixed_d = tuple([el.decode('utf-8') if type(el)
                                 is bytearray else el for el in d])
                cleaned_result.append(fixed_d)
        return cleaned_result

    def execute_db_query(self, query, parameters=()):
        """Execute the query with given parameters"""
        with self.wire:
            cursor = self.wire.connection.cursor(prepared=True)
            cursor.execute(query, parameters)
            query_result = self.clean_output(query, cursor)
            logger.info(
                "Query {} ran with arguments {}".format(query, parameters))
            logger.debug("Query returned {}".format(query_result))
        return query_result  # for select statements

    def create_table(self):
        query = """CREATE TABLE IF NOT EXISTS record (
                    Name varchar(60) NOT NULL,
                    CNIC varchar(13) DEFAULT NULL,
                    Gender varchar(1) DEFAULT NULL,
                    Status tinyint(4) DEFAULT '0',
                    Entry_Point varchar(45) DEFAULT NULL,
                    Entry_Time timestamp NULL DEFAULT CURRENT_TIMESTAMP,
                    Exit_Point varchar(45) DEFAULT NULL,
                    Exit_Time timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
                    KEY idx_cn (CNIC)
                    ) ENGINE=InnoDB DEFAULT CHARSET=latin1 """
        self.execute_db_query(query)

    def insert_person(self, parameters):
        query = """INSERT INTO record (Name,CNIC,Gender)VALUES(%s,%s,%s)"""
        self.execute_db_query(query, parameters)

    def update_status(self, parameters):
        query = """UPDATE record SET status=1
                           WHERE Entry_Time = %s"""
        self.execute_db_query(query, parameters)

    def view_record(self):
        query = """SELECT * FROM record ORDER BY record.Entry_Time ASC"""
        return self.execute_db_query(query)

    def check_status(self, parameters):
        query = """SELECT Exit_Time FROM record
                        WHERE Entry_Time = %s"""
        return self.execute_db_query(query, parameters)


if __name__ == '__main__':
    ex = OPsql()
    ex.create_table()
    for i in range(10):
        ex.insert_person(('pool test {}'.format(i), '43434', 'n'))
    logger.debug('Data enterd in the database')
    