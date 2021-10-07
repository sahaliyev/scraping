from turbo_db_operations import TurboDbOperations
import pandas as pd


class DataToCSV(TurboDbOperations):
    def __init__(self):
        super().__init__()

    def main(self):
        print("start")
        new_cars = []
        cars = self.get_cars()
        # for car in cars:
        #     car_id = car['id']
        #     details = self.get_separated_part_of_car('details', car_id)
        #     details = ", ".join(str(x[0]) for x in details)
        #     phone_numbers = self.get_separated_part_of_car('phone_number', car_id)
        #     phone_numbers = ", ".join(str(x[0]) for x in phone_numbers)
        #     car.append(details)
        #     car.append(phone_numbers)
        #     new_cars.append(car)

        df = pd.DataFrame(cars)
        # columns = self.get_table_names('car')
        # columns = [x[0] for x in columns]
        # columns.append('details')
        # columns.append('phone_numbers')
        columns = ['marka', 'model', 'price', 'year', 'city', 'engine', 'fuel_type', 'ban_type', 'color',
                   'used_by_km', 'engine_power', 'transmission', 'gearbox', 'loan', 'barter', 'view_count']
        # df = df[columns]
        df.columns = columns
        df.to_excel('excel/output.xlsx', sheet_name='Sheet1', index=False, header=True)
        with open('../excel/laylay.txt', 'a', encoding="utf-8") as file:
            print('addasdasd', file=file)
        print('done')


if __name__ == '__main__':
    DataToCSV().main()
