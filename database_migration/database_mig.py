import pdb

# from database_migration.handler import Helper
from handler import Helper


class DBMigration(Helper):
    def __init__(self):
        super().__init__()
        self.exists_count = 0

    @staticmethod
    def modify_item(car):
        car = dict(car)
        price = car.get('price', '0')
        price = price.replace(' ', '')
        car['price'] = int(price)
        engine_power = car.get('engine_power', '0')
        engine_power = engine_power.replace('a.g.', '').strip()
        car['engine_power'] = int(engine_power)
        used = car.get('used_by_km', '')
        used = used.replace('km', '').replace(' ', '')
        car['used_by_km'] = int(used)
        loan = car.get('loan')
        if loan == 'Yes':
            car['loan'] = 1
        else:
            car['loan'] = 0
        barter = car.get('barter')
        if barter == 'Yes':
            car['barter'] = 1
        else:
            car['barter'] = 0
        new = car.get('new')
        if new == 'Bəli':
            car['new'] = 1
        else:
            car['new'] = 0
        car['seller'] = car.pop('seller_name')
        car['is_parsed'] = 1
        return car

    def new_or_existence_id(self, table_name, value):
        if table_name == 'seller' and value:
            value = value.upper()  # this two lines makes seller name upper in order to prevent duplicates
        exists = self.item_exists(f'{table_name}', value)
        if exists:
            return exists[0]
        else:
            return self.insert_item_into_table(f'{table_name}', value)

    def main(self):
        while cars := self.get_cars_from_remote():
            fk_columns = ('ban_type', 'color', 'fuel_type', 'gearbox', 'marka',
                          'model', 'transmission', 'seller', 'city', 'currency')

            # cars = self.get_cars_from_remote()
            car_ids = [x['id'] for x in cars]
            details = self.get_items_from_remote('details', car_ids)
            images = self.get_items_from_remote('images', car_ids)
            phone_numbers = self.get_items_from_remote('phone_numbers', car_ids)
            for item in cars:
                old_car_id = item['id']
                unique_id = item.get('unique_id')
                if self.car_exists(unique_id):
                    self.exists_count += 1
                    continue
                m_item = self.modify_item(item)
                for key, value in m_item.items():
                    if key in fk_columns:
                        m_item[key] = self.new_or_existence_id(key, value)
                new_car_id = self.insert_into_car(m_item)

                car_details = [x for x in details if x['car_id'] == old_car_id]
                for detail in car_details:
                    unique_detail_id = self.new_or_existence_id('unique_details', detail['text'])
                    self.insert_into_details(unique_detail_id, new_car_id)

                car_images = [x for x in images if x['car_id'] == old_car_id]
                for image in car_images:
                    self.insert_into_images(image['url'], image['is_main'], new_car_id)

                car_phones = [x for x in phone_numbers if x['car_id'] == old_car_id]
                for phone in car_phones:
                    unique_phone_id = self.new_or_existence_id('unique_phone_number', phone['phone_number'])
                    self.insert_into_phone(unique_phone_id, new_car_id)
            self.mark_car_as_moved(car_ids)
            print("# of items new: ", 100 - self.exists_count)
            print("# of items exists: ", self.exists_count)


if __name__ == '__main__':
    DBMigration().main()
