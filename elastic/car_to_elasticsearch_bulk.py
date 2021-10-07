import sys

sys.path.append('../')

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from db_queries import DBQueries
from datetime import datetime


class CartoElastic(DBQueries):
    """
    New script that index item from database to elasticsearch.
    """

    def __init__(self):
        super().__init__()
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.indexed_count = 0

    def prepare_car(self, cars):
        # pdb.set_trace()
        car_ids = [x['id'] for x in cars]
        if not car_ids:
            return
        all_details = self.get_separated_part_of_car_bulk('details', car_ids)
        all_phone_numbers = self.get_separated_part_of_car_bulk('phone_number', car_ids)

        all_cars = []
        details_text_array = phone_numbers_text_array = []
        for car in cars:
            car_id = car['id']
            details_ids = [x['details'] for x in all_details if x['car_id'] == car_id]
            if details_ids:
                details_text_values = self.get_unique_values('unique_details', details_ids)
                details_text_array = [str(x[1]) for x in details_text_values]

            phone_numbers_ids = [x['phone_number'] for x in all_phone_numbers if x['car_id'] == car_id]
            if phone_numbers_ids:
                phone_numbers_text_values = self.get_unique_values('unique_phone_number', phone_numbers_ids)
                phone_numbers_text_array = [str(x[1]) for x in phone_numbers_text_values]

            body = {x: y for x, y in car.items() if x not in ('is_indexed', 'is_parsed', 'request_count')}
            body['details'] = details_text_array
            body['phone_numbers'] = phone_numbers_text_array
            if body.get('new'):
                body['new'] = 1
            else:
                body['new'] = 0
            if body.get('loan'):
                body['loan'] = 1
            else:
                body['loan'] = 0
            if body.get('barter'):
                body['barter'] = 1
            else:
                body['barter'] = 0
            all_cars.append(body)
        return all_cars

    def add_car_to_elastic(self, cars):
        actions = [
            {
                "_index": "car",
                "_id": car.get('id'),
                "_source": car
            }
            for car in cars
        ]

        try:
            helpers.bulk(self.es, actions)
            car_ids = [car.get('id') for car in cars]
            if car_ids:
                self.mark_as_inserted_to_elastic(car_ids)
                self.indexed_count = len(car_ids)
        except Exception as e:
            with open('logs/es.txt', 'a', encoding="utf-8") as file:
                print(f'ES error: {e}', file=file)

    def main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        # while cars := self.get_cars():  # this is for all data
        cars = self.get_cars()
        cars = self.prepare_car(cars)
        self.add_car_to_elastic(cars)
        print(f"Indexed: {self.indexed_count} items at {current_time}")


if __name__ == '__main__':
    CartoElastic().main()
