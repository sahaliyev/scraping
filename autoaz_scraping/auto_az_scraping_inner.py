import pdb
import sys

sys.path.append('../')

import random
import re
import traceback
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from emoji import UNICODE_EMOJI

import utils
from db_queries import DBQueries
from user_agents import user_agents

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class AutoAz(DBQueries):
    def __init__(self):
        super(AutoAz, self).__init__()
        self.base_url = 'http://www.auto.az'
        self.source_id = 2
        self.count = 0

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
        engine_fuel = car.pop('Mühərrik', None)
        if engine_fuel:
            engine, fuel = engine_fuel.split(',')
            pattern = r'\d+'
            res = re.search(pattern, engine)
            if res:
                res = res.group()
                car['engine'] = utils.normalize_engine(res)
            fuel = fuel.strip()

            try:
                car['fuel_type'] = utils.fuel_type_dict[fuel]
            except KeyError:
                car['fuel_type'] = fuel

        ban_type = car.pop('Ban tipi', None)
        try:
            car['ban_type'] = utils.ban_types_dict[ban_type]
        except KeyError:
            car['ban_type'] = ban_type

        gearbox = car.pop('Sürət qutusu', None)
        try:
            car['gearbox'] = utils.gearbox_dict[gearbox]
        except KeyError:
            car['gearbox'] = gearbox

        car['transmission'] = car.pop('Ötürücü tipi', None)

        car['description'] = f"Vəziyyəti: {car.get('Vəziyyəti', '')}. {car.get('Təsvir', '')}"

        car['marka'] = car.pop('Marka', None)
        car['model'] = car.pop('Model', None)
        car['engine_power'] = int(car.pop('Mühərrikin gücü', '0').replace('a.g.', '').strip())
        car['color'] = car.pop('Rəngi', None)

        car['date'] = car.pop('yerləşdirilmə tarixi', None)
        car['seller'] = car.pop('əlaqəli şəxs', None)
        car['phone_numbers'] = car.pop('Telefon', None)
        car['city'] = car.pop('region', None)
        car['details'] = car.pop('Əlavə təchizatlar', None)
        car['year'] = int(car.pop('Buraxılış ili', None))
        used_by_km = car.pop('Yürüş', None)
        car['used_by_km'] = int(used_by_km.replace(' km', ''))
        car['loan'] = car.pop('Kredit', None)

        return car

    @staticmethod
    def remove_emoji(description):
        if description:
            return re.sub(rx, u'', description)

    def parse_item_details(self, url):
        try:
            bs = self.get_beautiful_soup(url)
            car = {}
            container = bs.find('div', class_='el_n_self')

            container1, container2, _ = container.find_all('div', class_='eln_columns')

            # Below code takes images
            slider_container = container1.find('div', class_='eln_left')
            slider = slider_container.find('div', class_='sliders_s')
            images_list = []
            if slider:
                inner_slider = slider.find('div', class_='ow_h')
                if inner_slider:
                    images = inner_slider.find_all('div', class_='img_cont')
                    for item in images:
                        src = item.find('a').get('href')
                        images_list.append(self.base_url + src)
            car['images'] = images_list

            # product-properties all info about product
            product_properties = container.find('div', class_='eln_right')
            items = product_properties.find_all('tr')
            for item in items:
                name, value = item.find_all('td')
                if name and value:
                    name = name.text.strip()
                    value = value.text.strip()
                    car[name] = value

            #  description and details
            dd = container2.find('div', class_='eln_left')
            if dd and dd.find('div', class_='eln_desc'):
                titles = dd.find_all('b', class_='eln_title')
                values = dd.find_all('div', class_='eln_desc')
                for x in zip(titles, values):
                    key = x[0].text.strip()
                    value = x[1].text.strip()
                    car[key] = value

            # seller contact
            seller = container2.find('div', class_='eln_right')
            if seller:
                items = seller.find_all('tr')
                for item in items:
                    name, value = item.find_all('td')
                    if name and value:
                        name = name.text.strip()
                        value = value.text.strip()
                        car[name] = value

            return car

        except Exception as e:
            info = {'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc())}
            self.insert_into_logs(info)

    def parse_items(self, items):
        # pdb.set_trace()

        for car in items:
            car_id = car.get('id')
            request_count = car.get('request_count')
            try:
                car = dict(car)  # in order to be able to update, I convert db row to dict
                car.update(self.parse_item_details(car['url']))
                car = self.modify_car(car)
                car['description'] = self.remove_emoji(car['description'])

                if car['ban_type'] in ('kabriolet/rodster', 'miniven/mikroavtobus'):
                    continue

                if len(str(car['year'])) != 4:
                    car['year'] = None

                car['new'] = 1 if car['used_by_km'] == 0 else 0
                car['loan'] = 0 if car['loan'] == 'xeyr' else 1
                car['color'] = car['color'].replace('(metallik)', '').strip()
                if car['marka'] == 'Nissan Diesel':
                    car['marka'] = 'Nissan'

                fk_columns = ('city', 'ban_type', 'color', 'fuel_type', 'gearbox', 'marka',
                              'model', 'transmission', 'seller')
                for fk_item in fk_columns:
                    car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

                car['date'] = datetime.strptime(car['date'], '%d.%m.%Y')
                self.update_car(self.source_id, car)  # update item in database
                self.count += 1
                # insert inner images to table
                self.insert_into_images(car['images'], car_id)

                # insert details into table
                details = car.get('details')
                if details:
                    details_list = details.split(', ')
                    for detail_ in details_list:
                        try:
                            detail = utils.unique_details_dict[detail_]
                        except KeyError:
                            detail = detail_
                        unique_detail_id = self.new_or_existence_id('unique_details', detail)
                        self.insert_into_details(unique_detail_id, car_id)

                # insert phone numbers into table
                phone = car.get('phone_numbers')
                if phone:
                    unique_phone_number_id = self.new_or_existence_id('unique_phone_number', phone)
                    self.insert_into_phone_numbers(unique_phone_number_id, car_id)

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

            finally:
                self.increment_request_count(car_id, request_count)

    def auto_az_scraping_main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        items = self.get_unparsed_items(self.source_id)
        self.parse_items(items)
        print(f"Updated: {self.count} items at {current_time}")  # for cron log


if __name__ == '__main__':
    AutoAz().auto_az_scraping_main()
