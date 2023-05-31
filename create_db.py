import mysql.connector
from mysql.connector.errors import DatabaseError


class CreateDb:
    def __init__(self, user: str, password: str, database_name: str):
        """
        Create a local Mysqldb
        :param user: username of the Mysql
        :type user str
        :param password: password of the Mysql
        :type password: str
        :param database_name: name of the database we want to create
        :type database_name: str
        """
        self.user = user
        self.password = password
        self.db_name = database_name
        self.create()

    def create(self):
        """
        create a database with given information
        :return: if database Exist 'Database Already Exist'
        """
        try:

            my = mysql.connector.connect(host="localhost",
                                         user=self.user,
                                         passwd=self.password,
                                         )

            my_cur = my.cursor()

            my_cur.execute(f"CREATE DATABASE {self.db_name}")

            my_cur.execute("SHOW DATABASES")

            return 'Success'

        except DatabaseError:
            return 'Database Already Exist'



