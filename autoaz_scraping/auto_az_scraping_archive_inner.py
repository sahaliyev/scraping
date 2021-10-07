import pdb
import traceback
import sys
sys.path.append('../')
import requests
from bs4 import BeautifulSoup

import re
from datetime import datetime
from user_agents import user_agents
import random
from emoji import UNICODE_EMOJI
from auto_db_operations import AutoDbOperations
import helper_dict as hd
import pprint as pp

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class AutoAz(AutoDbOperations):
    def __init__(self):
        super(AutoAz, self).__init__()
        self.base_url = 'http://www.auto.az'
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
        del car['Buraxılış ili']
        del car['Kredit']
        del car['elanın nömrəsi']
        del car['Yürüş']

        fuel = car['Mühərrik'].split(',')[1].strip()
        del car['Mühərrik']

        car['fuel_type'] = hd.fuel_type.get(fuel)

        ban_type = car.pop('Ban tipi')
        car['ban_type'] = hd.ban_type.get(ban_type)

        gearbox = car.pop('Sürət qutusu')
        car['gearbox'] = hd.gearbox.get(gearbox)

        transmission = car.pop('Ötürücü tipi')
        car['transmission'] = hd.transmission.get(transmission)

        car['description'] = f"Vəziyyəti: {car.get('Vəziyyəti', '')}. {car.get('Təsvir', '')}"
        del car['Vəziyyəti']

        car['marka'] = car.pop('Marka')
        car['model'] = car.pop('Model')
        car['engine_power'] = int(car.pop('Mühərrikin gücü').replace('a.g.', '').strip())
        car['color'] = car.pop('Rəngi')

        car['date'] = car.pop('yerləşdirilmə tarixi')
        car['seller'] = car.pop('əlaqəli şəxs')
        car['phone_numbers'] = car.pop('Telefon')
        car['city'] = car.pop('region')
        car['details'] = car.pop('Əlavə təchizatlar')

        return car

    @staticmethod
    def remove_emoji(car):
        car['description'] = re.sub(rx, u'', car.pop('description'))
        return car

    def new_or_existence_id(self, table_name, value):
        if table_name == 'seller':
            value = value.upper()  # this two lines makes seller name upper in order to prevent duplicates
        exists = self.item_exists(f'{table_name}', value)
        if exists:
            return exists[0]
        else:
            return self.insert_item_into_table(f'{table_name}', value)

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
        for car in items:
            car_id = car.get('id')
            request_count = car.get('request_count')
            try:
                car = dict(car)  # in order to be able to update, I convert db row to dict
                car.update(self.parse_item_details(car['url']))

                car = self.modify_car(car)
                car = self.remove_emoji(car)
                fk_columns = ('color', 'marka', 'model', 'seller', 'city')
                for fk_item in fk_columns:
                    car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

                if type(car['ban_type']) == str:
                    car['ban_type'] = self.new_or_existence_id('ban_type', car.get('ban_type'))

                car['date'] = datetime.strptime(car['date'], '%d.%m.%Y')

                car['is_filtered'] = 1
                self.update_car_avto(car)  # update item in database
                self.count += 1
                # insert inner images to table
                self.insert_into_images_archive(car['images'], car_id)

                # insert details into table
                details = car.get('details')
                if details:
                    details_list = details.split(', ')
                    for detail_ in details_list:
                        try:
                            detail = hd.unique_details[detail_]
                        except KeyError:
                            detail = detail_
                        if type(detail) == int:
                            self.insert_into_details(detail, car_id)
                        else:
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
        items = self.get_unparsed_items_auto()
        self.parse_items(items)
        print(f"Updated: {self.count} items at {current_time}")  # for cron log


if __name__ == '__main__':
    AutoAz().auto_az_scraping_main()
