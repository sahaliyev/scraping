import sys

sys.path.append('../')
import pdb
import traceback

from db_operations import DbModel
from datetime import datetime


class TapAzDbOperations:
    def __init__(self):
        self.db = DbModel()

    #  below codes are for auto.az scraping
    def tap_az_exists(self, unique_id, source_id):
        sql = "SELECT id FROM car where unique_id = %s and source_id = %s"
        val = (unique_id, source_id)
        return self.db.get_single_item_from_db(sql, val)

    def current_car_price_currency(self, car_id):
        sql = "SELECT price, currency FROM car where id = %s"
        val = (car_id,)
        return self.db.get_single_item_from_db(sql, val)

    def price_and_currency_in_updated_car_table(self, car_id):
        sql = "SELECT price, currency_id FROM updated_car WHERE car_id = %s order by id desc limit 1"
        val = (car_id,)
        return self.db.get_single_item_from_db(sql, val)

    def insert_into_updated_car(self, car_id, price, currency_id, date):
        sql = "INSERT INTO updated_car (car_id, price, currency_id, date) VALUES (%s, %s, %s, %s)"
        val = (car_id, price, currency_id, date)
        self.db.insert_into_table(sql, val)

    def mark_car_as_updated_and_unfiltered(self, car_id):
        sql = "UPDATE car SET is_updated = 1, is_filtered = 0 WHERE id = %s"
        val = (car_id,)
        self.db.insert_into_table(sql, val)

    def insert_into_car_partial_tap_az(self, car):
        col_order = ('unique_id', 'city', 'date', 'currency', 'price', 'url', 'source_id')

        col_names = str(col_order).replace("'", "")
        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(col_order))
        sql = f"INSERT INTO car{col_names} VALUES({placeholders})"
        val = tuple(values)
        return self.db.insert_into_table(sql, val)

    def update_car_tapaz(self, car):
        col_order = ('marka', 'model', 'year', 'engine', 'fuel_type', 'ban_type',
                     'used_by_km', 'gearbox', 'description', 'view_count',
                     'new', 'seller', 'is_filtered')

        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = ' = %s, '.join(col_order)
        placeholder += ' = %s'
        sql = f"UPDATE car set {placeholder}, is_parsed=1 where id = %s"
        val = tuple(values) + (car.get('id'),)
        return self.db.insert_into_table(sql, val)

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
            with open('turbo_errors.txt', 'a', encoding="utf-8") as file:
                print(info, file=file)

    def insert_into_images_archive(self, images, last_inserted_car_id):
        for index, item in enumerate(images):
            if index == 0:
                sql = "INSERT INTO images (url, is_main, car_id) VALUES(%s, %s, %s) RETURNING id"
                val = (item, 1, last_inserted_car_id)
                self.db.insert_into_table_id_returning(sql, val)
            else:
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

    def increment_request_count(self, car_id, request_count):
        request_count += 1
        sql = "Update car set request_count = %s where id = %s"
        val = (request_count, car_id)
        self.db.insert_into_table(sql, val)

    def get_unparsed_items_tap(self):
        sql = "SELECT id, url, request_count " \
              "FROM car where source_id = 3 and is_parsed=0 and request_count < 5 order by id asc limit 100"
        return self.db.get_many_items_from_db_as_dict(sql, None)
