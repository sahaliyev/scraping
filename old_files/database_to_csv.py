from sqlalchemy import create_engine
import pandas as pd
import xlsxwriter


class FilterCars:
    @staticmethod
    def engine_creator():
        database = {
            'local': {
                'NAME': 'turbo',
                'USER': 'postgres',
                'PASSWORD': 'postgres',
                'HOST': 'localhost',
                'PORT': 5432,
            },
            'production': {
                'NAME': 'turbo',
                'USER': 'postgres',
                'PASSWORD': 'temp1Parol8#',
                'HOST': 'localhost',
                'PORT': 5432,
            }
        }
        db = database['production']
        engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
            user=db['USER'],
            password=db['PASSWORD'],
            host=db['HOST'],
            port=db['PORT'],
            database=db['NAME'],
        )

        return create_engine(engine_string)

    def filter_dataframe(self):
        engine = self.engine_creator()
        df = pd.read_sql_table('car', engine)
        df.to_excel("all_data.xlsx", index=False, sheet_name="all data")


if __name__ == '__main__':
    FilterCars().filter_dataframe()
