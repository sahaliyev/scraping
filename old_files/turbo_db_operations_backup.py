import pdb
import traceback

from db_operations import DbModel
from datetime import datetime


class TurboDbOperations:
    def __init__(self):
        self.db = DbModel()

    def car_exists(self, unique_id):
        """
        :param unique_id: in turbo.az all item has unique id
        :return: id of item if it exists
        """
        sql = "SELECT id FROM car where unique_id = %s"
        val = (unique_id,)
        return self.db.get_single_item_from_db(sql, val)

    def insert_into_car_partial(self, car):
        col_order = ('price', 'currency', 'unique_id', 'city', 'url', 'date')

        col_names = str(col_order).replace("'", "")
        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(col_order))
        sql = f"INSERT INTO car{col_names} VALUES({placeholders}) RETURNING id"
        val = tuple(values)
        return self.db.insert_into_table_id_returning(sql, val)

    def insert_into_car(self, car):
        col_order = ('name', 'marka', 'model', 'price', 'currency', 'unique_id', 'year', 'city',
                     'engine', 'engine_power', 'fuel_type', 'ban_type', 'color', 'used_by_km', 'transmission',
                     'gearbox', 'description', 'url', 'date', 'loan', 'barter', 'view_count',
                     'seller_name', 'new')

        col_names = str(col_order).replace("'", "")
        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(col_order))
        sql = f"INSERT INTO car{col_names} VALUES({placeholders}) RETURNING id"
        val = tuple(values)
        return self.db.insert_into_table_id_returning(sql, val)

    def get_unparsed_items(self):
        sql = "SELECT id, url, request_count " \
              "FROM car where is_parsed=0 and request_count < 5"
        val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def increment_request_count(self, car_id, request_count):
        request_count += 1
        sql = "Update car set request_count = %s where id = %s"
        val = (request_count, car_id)
        self.db.insert_into_table(sql, val)

    def update_car(self, car):
        col_order = ('marka', 'model', 'year', 'engine', 'fuel_type', 'ban_type', 'color', 'used_by_km',
                     'engine_power', 'transmission', 'gearbox', 'description', 'loan', 'barter',
                     'view_count', 'seller', 'new')
        col_order_zero = ('loan', 'barter')

        values = []
        for item in col_order:
            if item in col_order_zero:
                values.append(car.get(item, 0))
            else:
                values.append(car.get(item, None))

        placeholder = ' = %s, '.join(col_order)
        placeholder += ' = %s'
        sql = f"UPDATE car set {placeholder}, is_parsed=1 where id = %s"
        val = tuple(values) + (car.get('id'),)
        return self.db.insert_into_table(sql, val)

    def insert_into_images(self, image, last_inserted_car_id, is_main=False):
        if is_main:
            sql = "INSERT INTO images (url, is_main, car_id) VALUES(%s, %s, %s) RETURNING id"
            val = (image, 1, last_inserted_car_id)
            self.db.insert_into_table_id_returning(sql, val)
        else:
            for item in image:
                sql = "INSERT INTO images (url, is_main, car_id) VALUES(%s, %s, %s) RETURNING id"
                val = (item, 0, last_inserted_car_id)
                self.db.insert_into_table_id_returning(sql, val)

    def insert_into_details(self, unique_detail_id, car_id):
        sql = "INSERT INTO details (details, car_id) VALUES(%s, %s)"
        val = (unique_detail_id, car_id)
        self.db.insert_into_table(sql, val)

    def insert_into_phone_numbers(self, unique_phone_number_id, car_id):
        sql = "INSERT INTO phone_number (phone_number, car_id) VALUES(%s, %s)"
        val = (unique_phone_number_id, car_id)
        self.db.insert_into_table(sql, val)

    def get_cars(self):
        sql = "SELECT * FROM car WHERE is_indexed = 0 order by id asc limit 100"
        val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_today_cars(self):
        sql = "SELECT * FROM car WHERE date > '2020-10-26 00:00:00' order by id asc"
        val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def inserted_to_elastic(self, car_ids):
        val = tuple(car_ids)
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(val))
        sql = f"UPDATE car SET is_indexed = 1 where id in ({placeholders})"
        self.db.insert_into_table(sql, val)

    def get_separated_part_of_car(self, table_name, field_name, car_id):
        sql = f"Select {field_name} FROM {table_name} where car_id = %s"
        val = (car_id,)
        return self.db.get_many_items_from_db(sql, val)

    def get_table_names(self, table_name):
        sql = "SELECT column_name FROM information_schema.columns " \
              "WHERE table_schema = 'public' AND table_name   = %s"
        val = (table_name,)
        return self.db.get_many_items_from_db(sql, val)

    def get_requirements(self):
        sql = "SELECT * FROM requirements"
        val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def insert_into_logs(self, info):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = "INSERT INTO logs (link, exception_info, traceback_info, date) VALUES (%s, %s, %s, %s)"
        val = [x for x in info.values()]
        val.append(now)
        val = tuple(val)
        try:
            self.db.insert_into_table(sql, val)
        except Exception as e:
            info = {'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc()),
                    'date': now}
            with open('../turbo_errors.txt', 'a', encoding="utf-8") as file:
                print(info, file=file)

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

    def get_last_inserted_items(self, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        '''sql = f"SELECT date, engine, year, url, price, curr.currency, mk.marka, md.model, gb.gearbox, \
                bt.ban_type, ft.fuel_type FROM car \
                JOIN currency AS curr ON car.currency = curr.id \
                JOIN marka AS mk ON car.marka = mk.id \
                JOIN model AS md ON car.model = md.id \
                JOIN gearbox AS gb ON car.gearbox = gb.id \
                JOIN ban_type AS bt ON car.ban_type = bt.id \
                JOIN fuel_type AS ft ON car.fuel_type = ft.id WHERE car.id IN ({placeholders})"'''
        sql = f"SELECT marka, model, year, price, engine, gearbox, ban_type, fuel_type, description" \
              f" from car where id IN ({placeholders})"
        val = tuple(ids)
        return self.db.get_many_items_from_db_as_dict(sql, val)
