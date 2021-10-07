import pdb
import sys

sys.path.append('../')
import traceback

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from user_agents import user_agents
import random
from db_queries import DBQueries
from emoji import UNICODE_EMOJI
import math
import utils
import source

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class Rover(DBQueries):
    def __init__(self):
        super(Rover, self).__init__()
        self.source_id = source.ROVERAZ_SITE_ID
        self.base_url = 'https://rover.az'
        self.new_count = 0
        self.exists_count = 0
        self.price_updated_count = 0
        self.details_updated_count = 0
        self.totally_changed_count = 0

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
    def get_items(bs):
        container = bs.find('div', class_='mt-5')
        if container:
            container = container.find('div', class_='row')
            items = container.find_all('div', class_='col-lg-3')
            return items

    @staticmethod
    def parse_date(date):
        if date == 'Bugün':
            return datetime.now().strftime('%Y-%m-%d')
        elif date == 'Dünən':
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        else:  # 19 Jan 2021
            day, month_, year = date.split()
            month_ = datetime.now().month
            return datetime.strptime(f"{year}-{month_}-{day}", '%Y-%m-%d')

    def parse_items(self, items):
        for item in items:
            car = dict()
            try:
                url = item.a['href']
                car['url'] = self.base_url + url
                car['source_id'] = self.source_id

                _, marka, model, item_id = url.split('/')
                car['marka'] = marka.replace('+', ' ')
                car['model'] = model.replace('+', ' ')
                car['unique_id'] = int(item_id)

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

                # marka_that_has_series = ['mercedes', 'bmw', 'lexus']
                # if marka.lower() in marka_that_has_series:
                #     if model.upper() not in [x[0] for x in self.get_valid_models(marka_that_has_series)]:
                #         self.car_has_series_count += 1
                #         continue
                #
                # car['marka'] = self.new_or_existence_id('marka', marka)
                # car['model'] = self.new_or_existence_id('model', model)

                price_container = item.find('span', class_='ad-list-item-price')
                if price_container:
                    price_currency = price_container.get_text().strip()
                    pattern = r'[A-Za-z]+'
                    currency = re.search(pattern, price_currency)
                    if currency:
                        currency = currency.group()
                        currency_db = utils.currency_dict[currency]
                        price = int(price_currency.replace(currency, '').replace(' ', ''))
                        currency_id = self.new_or_existence_id('currency', currency_db)
                        rate = self.get_currency_rate(currency_id)[0]
                        car['price'] = math.ceil(price * rate)
                city = item.find('span', class_='float-left')
                if city:
                    city = city.get_text().strip()
                    car['city'] = self.new_or_existence_id('city', city)
                date = item.find('span', class_='float-right')
                if date:
                    date = date.get_text().strip()
                    car['date'] = self.parse_date(date)
                car['loan'] = 0
                loan = item.find('i', class_='fa-percent')
                if loan:
                    car['loan'] = 1
                car['barter'] = 0
                barter = item.find('i', class_='fa-sync-alt')
                if barter:
                    car['barter'] = 1

                info = item.find('div', class_='ad-list-item-info')
                if info:
                    info = info.get_text().strip()
                    # 1986 il, 1.5 L, 200 000 km
                    year, engine, used_by_km = info.split(',')
                    car['year'] = int(year.replace('il', '').strip())
                    car['engine'] = float(engine.replace('L', '').strip())
                    car['used_by_km'] = int(used_by_km.replace('km', '').replace(' ', ''))

                # get car name to check against db to see if the car has cahnged totally
                # name = item.find('div', class_='ad-list-item-name')
                # if name:
                #     name = name.get_text().strip().upper()

                car_exists = self.car_exists(self.source_id, car['unique_id'])
                if car_exists:
                    # increment exists_count if car is already exists but price changed
                    self.exists_count += 1
                    car_id, current_price = car_exists

                    # exists_name = self.get_car_name(car_id)
                    # if exists_name:
                    #     exists_name_str = f"{exists_name['marka']} {exists_name['model']}"
                    #     if exists_name_str != name:
                    #         self.totally_changed_count += 1
                    #         continue

                    e_year, e_engine, e_used_by_km = self.get_existence_year_engine_used_by_km(car_id)
                    if car['year'] != e_year or car['engine'] != e_engine or car['used_by_km'] != e_used_by_km:
                        self.update_car_year_engine_used_by_km(car_id, car['year'], car['engine'], car['used_by_km'])
                        self.details_updated_count += 1

                    if car['price'] == current_price:
                        continue
                    else:
                        updated_price = self.get_price_in_updated_car_price_table(car_id)
                        if updated_price:
                            updated_price = updated_price['price']
                            if updated_price != car['price']:
                                self.insert_into_updated_car_price(car_id, car['price'], car['date'])
                                self.mark_car_as_unfiltered(car_id)
                                self.price_updated_count += 1
                                continue
                            else:
                                continue
                        else:
                            self.insert_into_updated_car_price(car_id, car['price'], car['date'])
                            self.mark_car_as_unfiltered(car_id)
                            self.price_updated_count += 1
                            continue

                self.insert_into_car_partial(self.source_id, car)
                self.new_count += 1  # for logging

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

    def rover_az_scraping_main(self):
        # for index in range(1, 100):
        url = f'https://rover.az/son-elanlar?page=1'  # starting point
        bs = self.get_beautiful_soup(url)
        items = self.get_items(bs)
        self.parse_items(items)
        # for cron log
        print(f"New: {self.new_count}\n"
              f"Old: {self.exists_count}\n"
              f"Price updated: {self.price_updated_count}\n"
              f"Details updated: {self.details_updated_count}")
        # time.sleep(5)


if __name__ == '__main__':
    Rover().rover_az_scraping_main()
