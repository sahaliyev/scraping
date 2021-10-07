from elasticsearch import Elasticsearch
from elasticsearch import helpers
from turbo_db_operations import TurboDbOperations
from datetime import datetime


class CartoElastic(TurboDbOperations):
    """
    The script index today's items from database to elasticsearch.
    """
    def __init__(self):
        super().__init__()
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def prepare_car(self, cars):
        all_cars = []
        for car in cars:
            car_id = car['id']
            details = self.get_separated_part_of_car('details', car_id)
            details = [str(x[0]) for x in details]
            phone_numbers = self.get_separated_part_of_car('phone_number', car_id)
            phone_numbers = [str(x[0]) for x in phone_numbers]
            body = {x: y for x, y in car.items()}
            body['details'] = details
            body['phone_numbers'] = phone_numbers
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
                "_index": "test",
                "_id": car.get('id'),
                "_source": car
            }
            for car in cars
        ]

        try:
            helpers.bulk(self.es, actions)
        except Exception as e:
            print("Exception happened when inserting into elasticsearch: ", e)

    def main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        cars = self.get_today_cars()
        cars = self.prepare_car(cars)
        self.add_car_to_elastic(cars)
        print(f"# of items indexed: {len(cars)} at {current_time}")


if __name__ == '__main__':
    CartoElastic().main()
