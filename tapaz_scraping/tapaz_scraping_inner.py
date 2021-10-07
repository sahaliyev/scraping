import pdb
import sys

sys.path.append('../')
import traceback

import requests
from bs4 import BeautifulSoup

import re
from datetime import datetime
from user_agents import user_agents
import random
from emoji import UNICODE_EMOJI
from db_queries import DBQueries
import utils
import source

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class TapAz(DBQueries):
    def __init__(self):
        super(TapAz, self).__init__()
        self.base_url = 'https://tap.az'
        self.count = 0
        self.source_id = source.TAPAZ_SITE_ID

    @staticmethod
    def get_beautiful_soup(link):
        headers = {'user-agent': random.choice(user_agents)}
        response = requests.get(link, headers=headers)
        if response.status_code != 200:
            raise Exception(f'Request error. {response.status_code} {response.text}')
        response.encoding = "utf8"
        html = response.text
        bs = BeautifulSoup(html, 'lxml')
        return bs

    @staticmethod
    def modify_car(car):
        car['ban_type'] = car.pop('Kuzov növü', None)
        year = car.pop('Buraxılış ili', None)
        if year:
            car['year'] = int(year)
        engine = car.pop('Mühərrik, sm³', None)
        car['engine'] = engine
        car['gearbox'] = car.pop('Sürətlər qutusu', None)
        car['fuel_type'] = car.pop('Yanacaq növü', None)
        car['new'] = car.pop('Yeni?', None)
        used = car.pop('Yürüş, km', None)
        if used:
            car['used_by_km'] = int(used.replace('.', ''))
        car['view_count'] = int(car.pop('Baxışların sayı', None))
        car['marka'] = car.pop('Marka', None)
        car['model'] = car.pop('Model', None)
        return car

    @staticmethod
    def remove_emoji(description):
        if description:
            return re.sub(rx, u'', description)

    def parse_item_details(self, url):
        try:
            bs = self.get_beautiful_soup(url)
            car = dict()
            container = bs.find('div', id='js-lot-page')

            # Below code takes images
            slider = container.find('div', class_='photos')
            images_list = list()
            if slider:
                images = slider.find_all('a')
                for image in images:
                    src = image['href']
                    images_list.append(src)
            car['images'] = images_list

            # seller contact (person)
            seller = container.find('div', class_='aside-page')
            phones = list()
            if seller:
                author = seller.find('div', class_='author')
                if author:
                    phone = author.find('a', class_='phone')
                    if phone:
                        phones.append(phone.text.strip())
                    name = author.find('div', class_='name')
                    if name:
                        name = name.text.strip()
                        car['seller'] = name
                else:
                    car['seller'] = 'Sold'
                car['phone_numbers'] = phones

            # Below code takes statistics such as view count, update time and product id
            statistics = container.find('div', class_='lot-info')
            items = statistics.find_all('p')
            for item in items:
                key_value = item.text.strip()
                if 'Baxışların sayı' in key_value:
                    key, value = key_value.split(':')
                    car[key.strip()] = value.strip()

            # product-properties all info about product
            product_properties = container.find('div', class_='lot-right')
            table = product_properties.find('table', class_='properties')
            items = table.find_all('tr', class_='property')
            for item in items:
                key = item.find('td', class_='property-name')
                value = item.find('td', class_='property-value')
                if key and value:
                    car[key.text.strip()] = value.text.strip()

            #  description
            description = table.find_next_sibling('p')
            if description:
                description = description.text.strip()
                car['description'] = description

            return car

        except Exception as e:
            info = {'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc())}
            self.insert_into_logs(info)

    def parse_items(self, items):
        for car in items:
            car_id = car.get('id')
            request_count = car.get('request_count')
            try:
                car = dict(car)  # in order to be able to update, I convert db row to dict
                car.update(self.parse_item_details(car['url']))
                car = self.modify_car(car)
                car['description'] = self.remove_emoji(car['description'])

                ban_type = car['ban_type'].lower()
                try:
                    car['ban_type'] = utils.ban_types_dict[ban_type]
                except KeyError:
                    car['ban_type'] = ban_type

                try:
                    car['marka'] = utils.marka_dict[car['marka'].lower()]
                except KeyError:
                    car['marka'] = car['marka']

                try:
                    car['model'] = utils.model_dict[car['model'].lower()]
                except KeyError:
                    car['model'] = car['model']

                marka = self.get_marka_id(car['marka'])
                if not marka:
                    info = f"{car['marka']} is missing from source_id {self.source_id}"
                    with open('../logs/missing_marka_models.txt', 'a', encoding="utf-8") as file:
                        print(info, file=file)
                    continue
                model = self.get_model_id(marka['id'], car['model'])
                if not model:
                    info = f"{car['model']} of {car['marka']} is missing from source_id {self.source_id}"
                    with open('../logs/missing_marka_models.txt', 'a', encoding="utf-8") as file:
                        print(info, file=file)
                    continue

                car['mm'] = model['id']

                if len(str(car['year'])) != 4:
                    car['year'] = None

                car['engine'] = utils.normalize_engine(car['engine'])
                if not car['engine']:
                    continue

                fk_columns = ('fuel_type', 'gearbox', 'ban_type', 'seller')
                for fk_item in fk_columns:
                    car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

                self.update_car(self.source_id, car)  # update item in database
                self.count += 1
                # insert inner images to table
                self.insert_into_images(car['images'], car_id)

                # insert phone numbers into table
                for phone in car.get('phone_numbers'):
                    unique_phone_number_id = self.new_or_existence_id('unique_phone_number', phone)
                    self.insert_into_phone_numbers(unique_phone_number_id, car_id)

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

            finally:
                self.increment_request_count(car_id, request_count)

    def tap_az_scraping_main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        items = self.get_unparsed_items(self.source_id)
        self.parse_items(items)
        #  for cron log
        print(f"Parsed: {self.count} items at {current_time}")  # for cron log
        print('-' * 50)


if __name__ == '__main__':
    TapAz().tap_az_scraping_main()
