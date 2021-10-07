import pdb
import sys
import cx_Oracle


class DbModel:
    def __init__(self):
        if sys.platform == 'win32':
            cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\sahil.aliyev\oracle\instantclient_19_9")
        else:
            pass
            # cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\sahil.aliyev\oracle\instantclient_19_9")

        dsn = cx_Oracle.makedsn(
            host='172.16.10.243',
            port=1521,
            service_name='frdwh_pdb.hq.ferrumcapital.az'
        )

        self.dbconnection = cx_Oracle.connect(user='frdwhadmin', password='Baku$#2021', dsn=dsn)

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
        except cx_Oracle.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    def get_single_item_from_db_as_dict(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            item = cursor.fetchone()
            return item
        except cx_Oracle.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    def get_many_items_from_db(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            items = cursor.fetchall()
            return items
        except cx_Oracle.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    def get_many_items_from_db_as_dict(self, sql, val):
        # pdb.set_trace()
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            cursor.rowfactory = lambda *args: dict(zip([d[0] for d in cursor.description], args))
            items = cursor.fetchall()
            return items
        except cx_Oracle.DatabaseError as error:
            raise Exception("Failed fetching item {}".format(error))
        finally:
            cursor.close()

    def insert_into_table_id_returning(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            self.commit_db()
            return cursor.fetchone()[0]
        except cx_Oracle.DatabaseError as error:
            self.dbconnection.rollback()
            raise Exception("INSERT FAIlED!\n {}\n{}\n{}".format(sql, val, error))
        finally:
            cursor.close()

    def insert_into_table(self, sql, val):
        cursor = self.dbconnection.cursor()
        try:
            cursor.execute(sql, val)
            self.commit_db()
        except cx_Oracle.DatabaseError as error:
            self.dbconnection.rollback()
            raise Exception("INSERT FAIlED!\n {}\n{}\n{}".format(sql, val, error))
        finally:
            cursor.close()


if __name__ == '__main__':
    print(DbModel().dbconnection)
    # print(DbModel().rmconnection)
