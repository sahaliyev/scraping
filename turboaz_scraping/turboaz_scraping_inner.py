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
from db_queries import DBQueries
from emoji import UNICODE_EMOJI
import source

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class Turbo(DBQueries):
    def __init__(self):
        super(Turbo, self).__init__()
        self.source_id = source.TURBOAZ_SITE_ID
        self.base_url = 'https://turbo.az'
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
        car['ban_type'] = car.pop('Ban növü')
        car['horsepower'] = int(car.pop('Mühərrikin gücü').replace('a.g.', '').strip())
        car['color'] = car.pop('Rəng')
        car['gearbox'] = car.pop('Sürətlər qutusu')
        car['fuel_type'] = car.pop('Yanacaq növü')
        car['view_count'] = int(car.pop('ad_hits'))
        car['transmission'] = car.pop('Ötürücü')
        car['marka'] = car.pop('Marka')
        car['model'] = car.pop('Model')
        return car

    @staticmethod
    def remove_emoji(description):
        if description:
            return re.sub(rx, u'', description)

    def parse_item_details(self, url):
        try:
            bs = self.get_beautiful_soup(url)
            car = dict()
            container = bs.find('div', class_='product')

            # Below code takes images
            slider = container.find('div', class_='product-photos')
            images_list = list()
            if slider:
                images = slider.find_all('a')
                for image in images:
                    src = image['href']
                    images_list.append(src)
            car['images'] = images_list

            # Below code takes statistics such as view count, update time and product id
            statistics = container.find('div', class_='product-statistics')
            items = statistics.find_all('p')
            for item in items:
                label = item.find('label')
                if label.has_attr('for'):
                    name = label['for']
                    label.decompose()
                    car[name] = item.text.replace(':', '').strip()

            # product-properties all info about product
            product_properties = container.find('ul', class_='product-properties')
            items = product_properties.find_all('li')
            for item in items:
                name = item.find('label')
                if name:
                    ignore_list = ['Qiymət', 'Şəhər']
                    name = name.text.strip()
                    if name in ignore_list:
                        continue
                    value = item.find('div', class_='product-properties-value').text.strip()
                    car[name] = value

            # extra info
            extras = list()
            product_extras = container.find('div', class_='product-extras')
            if product_extras:
                items = product_extras.find_all('p', class_='product-extras-i')
                for item in items:
                    value = item.text.strip()
                    extras.append(value)
            car['details'] = extras

            # description
            car['description'] = None
            text = ""
            desc = container.find('h2', class_='product-text')
            while desc:
                desc = desc.find_next_sibling('p')
                if desc:
                    text += desc.text.strip()
            if text:
                car['description'] = text

            phones = list()
            # seller contact (person)
            seller = container.find('div', class_='seller-contacts')
            if seller:
                seller_name = seller.find('div', class_='seller-name').text.strip()
                phones_container = container.find('div', class_='seller-phone')
                items = phones_container.find_all('a', class_='phone')
                for item in items:
                    phone = item.text.strip()
                    phones.append(phone)
                car['seller'] = seller_name
            car['phone_numbers'] = phones

            # seller contact (shop)
            seller_shop = container.find('div', class_='shop-contact')
            if seller_shop:
                seller_name = seller_shop.find('a', class_='shop-contact--shop-name').text.strip()
                items = seller_shop.find_all('div', class_='shop-contact--phones-i')
                for item in items:
                    item = item.find('a', class_='shop-contact--phones-number')
                    phone = item.text.strip()
                    phones.append(phone)
                car['seller'] = seller_name
            car['phone_numbers'] = phones

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

                marka = self.get_marka_id(car['marka'])
                if not marka:
                    info = f"{car['marka']} is missing from source_id {self.source_id}"
                    with open('../logs/missing_marka_models.txt', 'a', encoding="utf-8") as file:
                        print(info, file=file)
                    continue
                model = self.get_model_id(marka['id'], car['model'])
                if not model:
                    info = f"{car['model']} of {car['marka']}is missing from source_id {self.source_id}"
                    with open('../logs/missing_marka_models.txt', 'a', encoding="utf-8") as file:
                        print(info, file=file)
                    continue

                car['mm'] = model['id']

                fk_columns = ('ban_type', 'color', 'fuel_type', 'gearbox', 'transmission', 'seller')
                for fk_item in fk_columns:
                    car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

                self.update_car(self.source_id, car)  # update item in database
                self.count += 1
                # insert inner images to table
                self.insert_into_images(car['images'], car_id)

                # insert details into table
                for detail in car.get('details'):
                    unique_detail_id = self.new_or_existence_id('unique_details', detail)
                    self.insert_into_details(unique_detail_id, car_id)

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

    def turbo_az_scraping_main(self):
        # pdb.set_trace()
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        items = self.get_unparsed_items(self.source_id)
        self.parse_items(items)
        print(f"Parsed: {self.count} items at {current_time}")  # for cron log
        print('-' * 50)


if __name__ == '__main__':
    Turbo().turbo_az_scraping_main()
