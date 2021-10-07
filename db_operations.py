import psycopg2
import psycopg2.extras
import sys

class DbModel:
    def __init__(self):

        if sys.platform == 'win32':
            self.dbconnection = psycopg2.connect(
                host='localhost',
                database='turbo',
                user='postgres',
                password='postgres')
        else:
            self.dbconnection = psycopg2.connect(
                host='172.16.10.222',
                database='turbo',
                user='postgres',
                password='Fe2Rum@)@)')

    def commit_db(self):
        self.dbconnection.commit()

    def close_db(self):
        self.dbconnection.close()

    def get_single_item_from_db(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            item = cursor.fetchone()
            return item
        except psycopg2.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    def get_single_item_from_db_as_dict(self, sql, val):
        cursor = self.dbconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            cursor.execute(sql, val)
            item = cursor.fetchone()
            return item
        except psycopg2.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    def get_many_items_from_db(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            items = cursor.fetchall()
            return items
        except psycopg2.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    """def get_many_items_from_db_remote(self, sql, val):
        cursor = self.rmconnection.cursor()
        try:
            cursor.execute(sql, val)
            items = cursor.fetchall()
            return items
        except psycopg2.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()"""

    def get_many_items_from_db_as_dict(self, sql, val):
        cursor = self.dbconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)  # DANGER CHAGE BACK
        try:
            cursor.execute(sql, val)
            items = cursor.fetchall()
            return items
        except psycopg2.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    """def get_many_items_from_db_as_dict_remote(self, sql, val):
        cursor = self.rmconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)  # DANGER CHAGE BACK
        try:
            cursor.execute(sql, val)
            items = cursor.fetchall()
            return items
        except psycopg2.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()"""

    def insert_into_table_id_returning(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            self.commit_db()
            return cursor.fetchone()[0]
        except psycopg2.DatabaseError as error:
            self.dbconnection.rollback()
            raise Exception("INSERT FAIlED!\n {}\n{}\n{}".format(sql, val, error))
        finally:
            cursor.close()

    def insert_into_table(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            self.commit_db()
        except psycopg2.DatabaseError as error:
            self.dbconnection.rollback()
            raise Exception("INSERT FAIlED!\n {}\n{}\n{}".format(sql, val, error))
        finally:
            cursor.close()


if __name__ == '__main__':
    print(DbModel().dbconnection)
    # print(DbModel().rmconnection)
