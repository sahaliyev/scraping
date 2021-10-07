import pdb

import pandas as pd
from datetime import datetime


class FilterCars:
    @staticmethod
    def str_value_to_int_converter(str_value):
        str_value = str_value.replace(" ", '')  # replacing for price is important
        return int(str_value)

    @staticmethod
    def extract_engine_size(engine, value):
        return value in engine

    @staticmethod
    def filter_df_marka(df, marka):
        return df[(df['marka'] == marka)]

    @staticmethod
    def filter_df_model(df, model):
        return df[(df['model'] == model)]

    def filter_df_year(self, df, year):
        min_year, max_year = year
        return df[(df['year'].apply(self.str_value_to_int_converter).between(min_year, max_year))]

    def filter_df_price(self, df, price):
        min_price, max_price = price
        return df[(df['price'].apply(self.str_value_to_int_converter).between(min_price, max_price))]

    @staticmethod
    def filter_df_gearbox(df, gearbox):
        gearbox = eval(gearbox)
        gearbox = [x for x in gearbox]  # list of gearbox
        return df[(df['gearbox'].isin(gearbox))]

    @staticmethod
    def filter_df_bay_type(df, ban_type):
        ban_type = eval(ban_type)
        ban_type = [x for x in ban_type]
        return df[(df['ban_type'].isin(ban_type))]

    @staticmethod
    def filter_df_fuel_type(df, fuel_type):
        fuel_type = eval(fuel_type)
        fuel_type = [x for x in fuel_type]
        return df[(df['fuel_type'].isin(fuel_type))]

    @staticmethod
    def filter_df_keywords(df):
        keywords = ['Təcili', 'Yeni gəlib', 'Öz avtomobilimdi', 'Bakıda sürülməyib',
                    'Pul lazımdır', 'İdeal vəziyyətdədir', 'Servis maşınıdır']

        return df[df['description'].str.contains('|'.join(keywords), case=False)]

    def filter_df_engine(self, df, engine):
        return df[df['engine'].apply(self.extract_engine_size, args=(str(engine),))]

    def filter_dataframe_without_keyword(self, df, **item):
        if item.get('marka'):
            df = self.filter_df_marka(df, item.get('marka'))
        if item.get('model'):
            df = self.filter_df_model(df, item.get('model'))
        if item.get('year'):
            df = self.filter_df_year(df, item.get('year'))
        if item.get('price'):
            df = self.filter_df_price(df, item.get('price'))
        if item.get('gearbox'):
            df = self.filter_df_gearbox(df, item.get('gearbox'))
        if item.get('ban_type'):
            df = self.filter_df_bay_type(df, item.get('ban_type'))
        if item.get('fuel_type'):
            df = self.filter_df_fuel_type(df, item.get('fuel_type'))
        if item.get('engine'):
            df = self.filter_df_engine(df, item.get('engine'))

        return df

    def filter_dataframe_with_keyword(self, df):
        df = self.filter_df_keywords(df)
        return df

    def filter_df_based_on_marka_model(self, df, **item):
        if item.get('marka'):
            df = self.filter_df_marka(df, item.get('marka'))
        if item.get('model'):
            df = self.filter_df_model(df, item.get('model'))
        return df

    def filter_cars_main(self, new_cars, requirements):
        columns = [x for x in new_cars[0].keys()]
        df = pd.DataFrame(new_cars, columns=columns)
        new_cars_with_keywords = []
        new_cars_without_keywords = []
        new_cars_marka_model = []
        for item in requirements:
            # df_without_keywords = self.filter_dataframe_without_keyword(df, **item)
            # new_cars_without_keywords.append(df_without_keywords)
            # df_with_keywords = self.filter_dataframe_with_keyword(df_without_keywords)
            # new_cars_with_keywords.append(df_with_keywords)
            res = self.filter_df_based_on_marka_model(df, **item)
            new_cars_marka_model.append(res)
        return pd.concat(new_cars_marka_model)
