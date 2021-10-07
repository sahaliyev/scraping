import traceback
from datetime import datetime

from db_operations import DbModel


class DBQueries:
    def __init__(self):
        self.db = DbModel()

    def car_exists(self, source_id, unique_id):
        sql = "SELECT id, price FROM car where source_id = %s and unique_id = %s"
        val = (source_id, unique_id)
        return self.db.get_single_item_from_db(sql, val)

    def mark_car_as_unfiltered(self, car_id):
        sql = "UPDATE car SET is_filtered = 0 WHERE id = %s"
        val = (car_id,)
        self.db.insert_into_table(sql, val)

    def insert_into_car_partial(self, source_id, car):
        if source_id == 1:
            col_order = ('price', 'unique_id', 'year', 'city', 'used_by_km', 'url',
                         'date', 'loan', 'barter', 'source_id', 'engine')
        elif source_id == 2:
            col_order = ('price', 'unique_id', 'year', 'city', 'used_by_km', 'url',
                         'date', 'loan', 'barter', 'source_id', 'engine', 'mm')
        elif source_id == 3:
            col_order = ('unique_id', 'city', 'date', 'price', 'url', 'source_id')

        col_names = str(col_order).replace("'", "")
        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(col_order))
        sql = f"INSERT INTO car{col_names} VALUES({placeholders})"
        val = tuple(values)
        return self.db.insert_into_table(sql, val)

    def get_unparsed_items(self, source_id):
        sql = "SELECT id, url, request_count " \
              "FROM car where is_parsed=0 and source_id = %s and request_count < 5 order by id asc"
        val = (source_id,)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def increment_request_count(self, car_id, request_count):
        request_count += 1
        sql = "Update car set request_count = %s where id = %s"
        val = (request_count, car_id)
        self.db.insert_into_table(sql, val)

    def update_car(self, source_id, car):
        if source_id == 1:
            col_order = ('fuel_type', 'ban_type', 'color', 'horsepower',
                         'transmission', 'gearbox', 'description', 'view_count', 'seller', 'mm')
        elif source_id == 2:
            col_order = ('fuel_type', 'ban_type', 'color', 'transmission', 'gearbox',
                         'description', 'view_count', 'seller')
        elif source_id == 3:
            col_order = ('year', 'engine', 'fuel_type', 'ban_type',
                         'used_by_km', 'gearbox', 'description', 'view_count', 'seller', 'mm')

        values = []
        for item in col_order:
            values.append(car.get(item, None))

        placeholder = ' = %s, '.join(col_order)
        placeholder += ' = %s'
        sql = f"UPDATE car set {placeholder}, is_parsed=1 where id = %s"
        val = tuple(values) + (car.get('id'),)
        return self.db.insert_into_table(sql, val)

    def insert_into_cars(self, car):
        columns = ('url', 'source_id', 'loan', 'barter', 'description',
                   'seller', 'ban_type', 'horsepower', 'color', 'gearbox', 'fuel_type', 'view_count',
                   'transmission', 'price', 'city', 'year', 'date', 'unique_id',
                   'engine', 'used_by_km', 'mm', 'is_parsed')

        values = []
        for item in columns:
            values.append(car.get(item, None))

        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(values))
        cols = str(columns).replace("'", "")

        sql = f'insert into car {cols} values ({placeholders}) RETURNING id'
        val = tuple(values)
        return self.db.insert_into_table_id_returning(sql, val)

    def insert_into_images(self, images, last_inserted_car_id):
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

    def get_cars(self):
        sql = "SELECT car.id, mk.marka, md.model, price, unique_id, \
                year, ct.city, engine,  ft.fuel_type, bt.ban_type, cl.color,\
                used_by_km, engine_power, tm.transmission, gb.gearbox,\
                description, url, date, loan, barter, view_count, new, sl.seller\
                FROM car \
                JOIN marka AS mk ON car.marka = mk.id\
                JOIN model AS md ON car.model = md.id \
                JOIN city AS ct ON car.city = ct.id\
                JOIN fuel_type AS ft ON car.fuel_type = ft.id\
                LEFT JOIN ban_type AS bt ON car.ban_type = bt.id\
                LEFT JOIN color AS cl ON car.color = cl.id\
                LEFT JOIN transmission AS tm ON car.transmission = tm.id\
                JOIN gearbox AS gb ON car.gearbox = gb.id\
                JOIN seller AS sl ON car.seller = sl.id\
                WHERE is_parsed = 1 and is_indexed = 0"
        val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def mark_as_inserted_to_elastic(self, car_ids):
        val = tuple(car_ids)
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(val))
        sql = f"UPDATE car SET is_indexed = 1 where id in ({placeholders})"
        self.db.insert_into_table(sql, val)

    def get_separated_part_of_car(self, table_and_field_name, car_id):
        sql = f"Select {table_and_field_name} FROM {table_and_field_name} where car_id = %s"
        val = (car_id,)
        return self.db.get_many_items_from_db(sql, val)

    def get_separated_part_of_car_bulk(self, table_name, ids):
        """
        Car has parts such as details, phone_numbers, images that are in different table.
        In some case we need car as whole therefore we need to bring all data together.
        :param table_name: can be images, details, phone_number
        :param ids: id of cars that we want to get seperated parts of
        :return: list of all items. Further selection should be done for each car
        """
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        sql = f"select * from {table_name} where car_id IN ({placeholders})"
        val = tuple(ids)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_unique_values(self, table_name, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        sql = f"select * from {table_name} where id IN ({placeholders})"
        val = tuple(ids)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_table_names(self, table_name):
        sql = "SELECT column_name FROM information_schema.columns " \
              "WHERE table_schema = 'public' AND table_name   = %s"
        val = (table_name,)
        return self.db.get_many_items_from_db(sql, val)

    def get_requirements(self):
        sql = "SELECT * FROM requirements"
        val = None
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_corresponding_requirements(self, mm):
        sql = "SELECT * FROM requirements where mm = %s"
        val = (mm,)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_part_of_requirement(self, table_name, req_id):
        sql = f"SELECT * FROM {table_name} where requirement_id = %s"
        val = (req_id,)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_requirement_value(self, table_name, db_pk):
        sql = f"SELECT {table_name} FROM {table_name} where id = %s"
        val = (db_pk,)
        return self.db.get_single_item_from_db_as_dict(sql, val)

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
            with open('../sql_errors.txt', 'a', encoding="utf-8") as file:
                print(info, file=file)

    def item_exists(self, table_column_name, look_up_value):
        """
        :param table_column_name: table and column name are the same
        :param look_up_value: this is the value we are looking for in the table
        :return: id if value in table
        """
        if look_up_value:
            sql = f"SELECT id FROM {table_column_name} WHERE {table_column_name} = %s"
        else:
            sql = f"SELECT id FROM {table_column_name} WHERE {table_column_name} IS NULL"
        val = (look_up_value,)
        return self.db.get_single_item_from_db_as_dict(sql, val)

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

    def join_car_to_parts_query(self, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        sql = f"SELECT car.id, date, engine, year, url, price, used_by_km," \
              f"ft.fuel_type, mm.model, mk.marka, car.mm " \
              f"FROM car " \
              f"JOIN fuel_type AS ft ON car.fuel_type = ft.id " \
              f"JOIN model as mm on mm.id = car.mm " \
              f"JOIN marka as mk on mk.id = mm.marka_id " \
              f"WHERE car.id IN ({placeholders})"
        val = tuple(ids)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_average_price_based_on_marka_and_model_and_year(self, mm, year):
        sql = f"SELECT cast(avg(price) as integer) FROM car WHERE mm = %s and year = %s"
        val = (mm, year)
        return self.db.get_single_item_from_db(sql, val)

    def get_currency_rate(self, currency_id):
        sql = 'SELECT rate FROM exchange_rate WHERE currency_id = %s'
        val = (currency_id,)
        return self.db.get_single_item_from_db(sql, val)

    def get_unfiltered_items(self):
        sql = f"SELECT id, mm, year, price, used_by_km, engine, " \
              f"fuel_type, url" \
              f" FROM car WHERE is_parsed = 1 and is_filtered = 0 order by date"
        return self.db.get_many_items_from_db_as_dict(sql, None)

    def mark_item_as_filtered(self, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        sql = f"UPDATE car SET is_filtered = 1 WHERE id IN ({placeholders})"
        val = tuple(ids)
        self.db.insert_into_table(sql, val)

    def get_unfiltered_items_test(self):
        sql = f"SELECT id, mm, year, price, engine, " \
              f"gearbox, ban_type, fuel_type, url" \
              f" FROM car where is_parsed = 1"
        return self.db.get_many_items_from_db_as_dict(sql, None)

    def update_exchange_rate(self, currency_id, value):
        sql = "UPDATE exchange_rate SET rate = %s where currency_id = %s"
        val = (value, currency_id)
        self.db.insert_into_table(sql, val)

    def get_all_requirements(self):
        sql = """SELECT req.id, min_year, max_year, min_price, max_price, mk.marka, md.model, engine
                 FROM requirements AS req
                 JOIN marka AS mk ON req.marka = mk.id
                 JOIN model AS md ON req.model = md.id
                 ORDER BY req.id
               """
        return self.db.get_many_items_from_db_as_dict(sql, None)

    def get_joined_part_of_requirements(self, name, ids):
        placeholder = '%s'
        placeholders = ', '.join([placeholder] * len(ids))

        sql = f"SELECT {name} FROM {name} WHERE id IN ({placeholders})"
        val = tuple(ids)
        return self.db.get_many_items_from_db(sql, val)

    def get_model_of_marka(self, marka_id):
        sql = "select distinct(md.model) from car " \
              "join model as md on car.model = md.id where marka=%s"
        val = (marka_id,)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def new_or_existence_id(self, table_name, value):
        if type(value) == str:
            value = value.upper().strip()
        exists = self.item_exists(f'{table_name}', value)
        if exists:
            return exists['id']
        else:
            return self.insert_item_into_table(f'{table_name}', value)

    def update_exchange_rate(self, id_, value):
        sql = "UPDATE exchange_rate SET rate = %s where id = %s"
        val = (value, id_)
        self.db.insert_into_table(sql, val)

    def mark_car_as_unparsed_unfiltered(self, car_id, is_price_updated=False):
        if is_price_updated:
            sql = 'update car set is_parsed = 0, request_count = 0, is_filtered = 0 where id = %s'
        else:
            sql = 'update car set is_parsed = 0, request_count = 0 where id = %s'
        val = (car_id,)
        self.db.insert_into_table(sql, val)

    def get_valid_models(self, marka_that_has_series):
        dd = ' or '.join(('marka=%s ' * len(marka_that_has_series)).split())

        sql = f"select distinct md.model from car as c " \
              f"join model as md on c.model = md.id " \
              f"where marka in (select id from marka where {dd})"
        val = tuple([x.upper() for x in marka_that_has_series])
        return self.db.get_many_items_from_db(sql, val)

    def get_price_in_updated_car_price_table(self, car_id):
        sql = 'select price from updated_car_price where car_id = %s order by date desc limit 1'
        val = (car_id,)
        return self.db.get_single_item_from_db_as_dict(sql, val)

    def insert_into_updated_car_price(self, car_id, new_price, date):
        sql = 'insert into updated_car_price (car_id, price, date) values (%s, %s, %s)'
        val = (car_id, new_price, date)
        self.db.insert_into_table(sql, val)

    def update_car_year_engine_used_by_km(self, car_id, year, engine, used_by_km, date):
        sql = 'update car set year = %s, engine = %s, used_by_km = %s, date = %s where id = %s'
        val = (year, engine, used_by_km, date, car_id)
        self.db.insert_into_table(sql, val)

    def get_existence_year_engine_used_by_km(self, car_id):
        sql = 'select year, engine, used_by_km from car where id = %s'
        val = (car_id,)
        return self.db.get_single_item_from_db(sql, val)

    def insert_into_model(self, marka_id, model):
        sql = "insert into model (marka_id, model) values (%s, %s)"
        val = (marka_id, model)
        self.db.insert_into_table(sql, val)

    def model_exists(self, marka_id, model):
        sql = f"SELECT id FROM model WHERE marka_id = %s and model = %s"
        val = (marka_id, model)
        return self.db.get_single_item_from_db_as_dict(sql, val)

    def new_or_existence_model(self, marka_id, model):
        model = model.upper()
        exists = self.model_exists(marka_id, model)
        if exists:
            return exists['id']
        else:
            self.insert_into_model(marka_id, model)

    def get_marka_id(self, marka):
        marka = marka.upper()
        sql = 'select id from marka where marka = %s'
        val = (marka,)
        return self.db.get_single_item_from_db_as_dict(sql, val)

    def get_model_id(self, marka_id, model):
        model = model.upper()
        sql = 'select id from model where marka_id = %s and model = %s'
        val = (marka_id, model)
        return self.db.get_single_item_from_db_as_dict(sql, val)

    def get_old_marka_model(self):
        sql = '''select car.id, mk.marka, md.model from car 
                 join marka_old as mk on mk.id = car.marka
                 join model_old as md on md.id = car.model
                 where car.mm is null and is_parsed = 1 and car.id > 418305
                 and md.model is not null
                 order by car.id'''
        return self.db.get_many_items_from_db_as_dict(sql, None)

    def update_car_mm(self, car_id, mm_id):
        sql = 'update car set mm = %s where id = %s'
        val = (mm_id, car_id)
        self.db.insert_into_table(sql, val)

    def send_mail_marketing_query(self):
        sql = """select car.id, car.price, car.year, mk.marka, md.model, sl.seller, url from car
                 join model as md on md.id = car.mm
                 join marka as mk on mk.id = md.marka_id
                 join seller as sl on sl.id = car.seller
                 where md.marka_id in (95, 122, 39, 216) 
                 and source_id = 1 and car.year >= 2005 and car.city in (1, 2, 3) 
                 and date >= %s order by date asc"""
        dd = datetime.now().strftime("%Y-%m-%d")
        # dd = '2021-06-07'
        # dd1 = '2021-06-08'

        val = (dd,)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_car_phone_numbers(self, car_id):
        sql = '''select unique_phone_number from unique_phone_number 
                where id in (select phone_number from phone_number where car_id = %s)'''
        val = (car_id,)
        return self.db.get_many_items_from_db_as_dict(sql, val)

    def get_car_name(self, car_id):
        sql = '''select mk.marka, md.model from car 
                    join model as md on md.id = car.mm
                    join marka as mk on mk.id = md.marka_id
                    where car.id = %s'''
        val = (car_id,)
        return self.db.get_single_item_from_db_as_dict(sql, val)

    def get_receiver_emails(self):
        sql = 'select email from receiver_emails'
        return self.db.get_many_items_from_db(sql, None)
