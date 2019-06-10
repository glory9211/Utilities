"""
Using MySQL COnnector and making a Class connect so that
we can use the python 'with' statement to create connections
"""

import mysql.connector
from mysql.connector import Error
import logging


connection_config_dict = {
    'user': '',  					# Write Username (root/admin)
    'password': '',  				# Enter password
    'host': '127.0.0.1',  			# Enter local host address or your website address
    'database': 'projectsql',  		# Name the database to use
    'raise_on_warnings': False,  	# Chack these options online
    'autocommit': True,
    'pool_name': 'mypool',
    'pool_size': 5,
}

# Logger setup

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formater = logging.Formatter('%(asctime)s:%(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('connector_test.log')
file_handler.setFormatter(formater)

logger.addHandler(file_handler)

# Logger setup ended


class ConnectorUtil():
    """ Class to create connection with MySQL and
        to make it useable using 'with' keyword"""

    def __enter__(self):
        """ Entring function show database name and
            provides a cusrosr for executing queries """
        try:
            self.connection = mysql.connector.connect(**connection_config_dict)
            if self.connection.is_connected():
                db_Info = self.connection.get_server_info()
                logger.debug(
                    "Connected to MySQL database... {}".format(db_Info))
                cursor = self.connection.cursor()
                cursor.execute("select database();")
                record = cursor.fetchone()
                logger.info("You are connected to - {}".format(record))
        except Error as e:
            logger.error("Error while connecting to MySQL: {}".format(e))

    def __exit__(self, *args):
        """ Closing connection and exiting """
        try:
            if(self.connection.is_connected()):
                self.connection.close()
                logger.debug("MySQL self.connection is closed")
        except Error as e:
            logger.error("Error while connecting to MySQL: {}".format(e))


if __name__ == '__main__':
    ex = ConnectorUtil()
    """For Proper Usage See modelsql file in the same directory"""
    with ex:
        print('Entering')
