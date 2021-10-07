import sys
import pdb

sys.path.append('../')
from db_operations import DbModel


class Helper:
    def __init__(self):
        self.db = DbModel()

    def get_cars_from_remote(self):
        sql = "select * from car where is_moved = 0 order by id asc limit 100"
        return self.db.get_many_items_from_db_as_dict(sql, val=None)

    def get_items_from_remote(self, table_name, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        sql = f"select * from {table_name} where car_id IN ({placeholders})"
        val = tuple(ids)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def item_exists(self, table_column_name, look_up_value):
        """
        :param table_column_name: table and column name are the same
        :param look_up_value: this is the value we are looking for in the table
        :return: id if value in table
        """
        sql = f"SELECT id FROM {table_column_name} where {table_column_name} = %s"
        val = (look_up_value,)
        return self.db.get_single_item_from_db(sql, val)

    def insert_item_into_table(self, table_and_column_name, inserted_value):
        """
        :param table_and_column_name: table and column name are the same
        :param inserted_value: value that we gonna insert into table
        :return: id of inserted item
        """
        col_name = f"({table_and_column_name})"
        sql = f"INSERT INTO {table_and_column_name} {col_name} VALUES(%s) RETURNING id"
        val = (inserted_value,)
        return self.db.insert_into_table_id_returning(sql, val)

    def car_exists(self, unique_id):
        """
        :param unique_id: in turbo.az all item has unique id
        :return: id of item if it exists
        """
        sql = "SELECT id FROM car where unique_id = %s"
        val = (unique_id,)
        return self.db.get_single_item_from_db(sql, val)

    def insert_into_car(self, car):
        col_order = ('marka', 'model', 'price', 'currency', 'unique_id', 'year', 'city',
                     'engine', 'fuel_type', 'ban_type', 'color', 'used_by_km', 'engine_power',
                     'transmission', 'gearbox', 'description', 'url', 'date', 'loan', 'barter',
                     'view_count', 'new', 'is_indexed', 'is_parsed', 'seller')

        col_names = str(col_order).replace("'", "")
        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(col_order))
        sql = f"INSERT INTO car{col_names} VALUES({placeholders}) RETURNING id"
        val = tuple(values)
        return self.db.insert_into_table_id_returning(sql, val)

    def insert_into_details(self, unique_detail_id, car_id):
        sql = "INSERT INTO details (details, car_id) VALUES(%s, %s)"
        val = (unique_detail_id, car_id)
        self.db.insert_into_table(sql, val)

    def insert_into_images(self, url, is_main, car_id):
        sql = "INSERT INTO images (url, is_main, car_id) VALUES(%s, %s, %s)"
        val = (url, is_main, car_id)
        self.db.insert_into_table(sql, val)

    def insert_into_phone(self, number, car_id):
        sql = "INSERT INTO phone_number (phone_number, car_id) VALUES(%s, %s)"
        val = (number, car_id)
        self.db.insert_into_table(sql, val)

    def mark_car_as_moved(self, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))
        sql = f"update car set is_moved = 1 where id IN ({placeholders})"
        val = tuple(ids)
        return self.db.insert_into_remote(sql, val)
