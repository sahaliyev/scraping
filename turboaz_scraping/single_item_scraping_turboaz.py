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
import math
import argparse

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
        try:
            car['horsepower'] = int(car.pop('Mühərrikin gücü', '').replace('a.g.', ''))
        except ValueError:
            car['horsepower'] = None
        car['color'] = car.pop('Rəng')
        car['gearbox'] = car.pop('Sürətlər qutusu')
        car['fuel_type'] = car.pop('Yanacaq növü')
        car['view_count'] = int(car.pop('ad_hits'))
        car['transmission'] = car.pop('Ötürücü')
        car['marka'] = car.pop('Marka')
        car['model'] = car.pop('Model')
        car['price'] = car.pop('Qiymət')
        car['city'] = car.pop('Şəhər')
        car['year'] = car.pop('Buraxılış ili')
        car['new'] = car.pop('Yeni')
        try:
            car['date'] = car['date'] = datetime.strptime(car.pop('ad_updated_at'), '%d.%m.%Y')
        except ValueError:
            car['date'] = None
        try:
            car['unique_id'] = int(car.pop('ad_id'))
        except ValueError:
            car['unique_id'] = None
        try:
            car['engine'] = float(car.pop('Mühərrik', '').replace('L', ''))
        except ValueError:
            car['engine'] = None
        try:
            car['used_by_km'] = int(car.pop('Yürüş', '').replace('km', '').replace(' ', ''))
        except ValueError:
            car['used_by_km'] = None
        return car

    @staticmethod
    def remove_emoji(description):
        if description:
            return re.sub(rx, u'', description)

    def parse_item_inner(self, bs):
        try:
            # bs = self.get_beautiful_soup(url)
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
                    name = name.text.strip()
                    value = item.find('div', class_='product-properties-value').text.strip()
                    car[name] = value
            # take loan and barter info as well
            car['loan'] = 0
            if product_properties.find('li', class_='product-properties-i_loan'):
                car['loan'] = 1
            car['barter'] = 0
            if product_properties.find('li', class_='product-properties-i_barter'):
                car['barter'] = 1

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

    def normalize_price(self, price_currency):
        # 16 900 AZN
        list_of_items = price_currency.split()
        currency = list_of_items.pop(-1)

        price = int(''.join(list_of_items))

        if currency:
            currency_id = self.new_or_existence_id('currency', currency)
            rate = self.get_currency_rate(currency_id)[0]
            return math.ceil(price * rate)

    def parse_item(self, url):
        bs = self.get_beautiful_soup(url)
        # car_id = car.get('id')
        # request_count = car.get('request_count')
        car = dict()
        try:
            car['url'] = url
            try:
                item_id = int(url.split('/')[-1].split('-')[0])
            except Exception as e:
                return

            car['source_id'] = self.source_id
            if self.car_exists(self.source_id, item_id):
                return

            car.update(self.parse_item_inner(bs))
            car = self.modify_car(car)
            car['price'] = self.normalize_price(car['price'])

            if car['new'] == 'Xeyr':
                car['new'] = 0
            else:
                car['new'] = 1

            car['description'] = self.remove_emoji(car['description'])

            marka = self.get_marka_id(car['marka'])
            if not marka:
                return
            model = self.get_model_id(marka['id'], car['model'])
            if not model:
                return

            car['mm'] = model['id']

            fk_columns = ('ban_type', 'color', 'fuel_type', 'gearbox', 'transmission', 'seller', 'city')
            for fk_item in fk_columns:
                car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

            # self.update_car(self.source_id, car)  # update item in database
            # insert car and take its id in order to insert car details
            car['is_parsed'] = 1
            car_id = self.insert_into_cars(car)
            # insert inner images to table
            # pdb.set_trace()
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
            link = url
            info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc())}
            self.insert_into_logs(info)

    def turbo_az_scraping_main(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-u', '--url', help='Turbo az single car url to parse', required=True, type=str)
        args = vars(parser.parse_args())

        url = args.get('url')
        if url:
            self.parse_item(url)


if __name__ == '__main__':
    Turbo().turbo_az_scraping_main()
