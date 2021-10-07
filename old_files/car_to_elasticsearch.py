from elasticsearch import Elasticsearch
from turbo_db_operations import TurboDbOperations
from datetime import datetime


class CartoElastic(TurboDbOperations):
    """
    Purpose of the script is to index items from databsse to elasticsearch.
    However, it does it one by one and I updated the script. New script name is car_to_elasticsearch_bulk.py
    """
    def __init__(self):
        super().__init__()
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
        self.car_added = 0

    def add_car_to_elastic(self, cars):
        for car in cars:
            car_id = car['id']
            details = self.get_separated_part_of_car('details', 'text', car_id)
            details = [str(x[0]) for x in details]
            phone_numbers = self.get_separated_part_of_car('phone_numbers', 'phone_number', car_id)
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
            try:
                self.es.index(index='car', id=car_id, body=body)
                self.inserted_to_elastic(car_id)
                self.car_added += 1
            except Exception as e:
                print("Exception happened when inserting into elasticsearch: ", e)

    def main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        cars = self.get_cars()
        self.add_car_to_elastic(cars)
        print(f"# of items indexed: {self.car_added} at {current_time}")


if __name__ == '__main__':
    CartoElastic().main()
