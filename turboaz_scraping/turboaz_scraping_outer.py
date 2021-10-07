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
import source

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class Turbo(DBQueries):
    def __init__(self):
        super(Turbo, self).__init__()
        self.source_id = source.TURBOAZ_SITE_ID
        self.base_url = 'https://turbo.az'
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
        titles = bs.find_all('p', class_='section-title_name')
        title_parent = None
        for title in titles:
            if title.text.strip() == 'ELANLAR':
                title_parent = title.parent
        if title_parent:
            items_container = title_parent.find_next_sibling('div', class_='products')
            items = items_container.find_all('div', class_='products-i')
            return items

    def parse_datetime(self, value):
        try:
            value = value.strip()
            if 'bugün' in value:
                _, time = value.split()
                date = datetime.now().date().strftime('%d.%m.%Y')
                return datetime.strptime(f"{date} {time}", '%d.%m.%Y, %H:%M')
            elif 'dünən' in value:
                _, time = value.split()
                date = (datetime.now().date() - timedelta(days=1)).strftime('%d.%m.%Y')
                return datetime.strptime(f"{date} {time}", '%d.%m.%Y, %H:%M')
            else:
                return datetime.strptime(value, '%d.%m.%Y, %H:%M')
        except Exception as e:
            return datetime.now()

    def parse_items(self, items):
        # pdb.set_trace()
        for item in items:
            car = dict()
            try:
                url = item.a['href']
                item_id = url.replace('/autos/', '').split('-')[0]
                car['unique_id'] = int(item_id)

                car['url'] = self.base_url + url
                car['source_id'] = self.source_id

                price_container = item.find('div', class_='product-price')
                currency_span = price_container.find('span')
                currency = currency_span.text.strip()
                currency_id = self.new_or_existence_id('currency', currency)
                rate = self.get_currency_rate(currency_id)[0]
                currency_span.decompose()
                price = price_container.text.strip()
                price = int(price.replace(' ', ''))
                car['price'] = math.ceil(price * rate)
                place_date = item.find('div', class_='products-i__datetime').text.strip()
                place, date = place_date.split(',', 1)  # date still has empty space at the beginning
                car['city'] = self.new_or_existence_id('city', place)
                car['date'] = self.parse_datetime(date)
                name = item.find('div', class_='products-i__name')
                if name:
                    name = name.get_text().strip().upper()

                car['loan'] = 0
                loan = item.find('div', class_='products-i__icon--loan')
                if loan:
                    car['loan'] = 1
                car['barter'] = 0
                barter = item.find('div', class_='products-i__icon--barter')
                if barter:
                    car['barter'] = 1

                details = item.find('div', class_='products-i__attributes')
                if details:
                    year, engine, used_by_km = details.text.strip().split(',')
                    # year = year.get_text().strip()
                    car['year'] = int(year.replace('il', '').strip())
                    # engine = engine.get_text().strip()
                    car['engine'] = float(engine.replace('L', '').strip())
                    # used_by_km = used_by_km.get_text().strip()
                    car['used_by_km'] = int(used_by_km.replace('km', '').replace(' ', '').strip())

                car_exists = self.car_exists(self.source_id, car['unique_id'])
                if car_exists:
                    # increment exists_count if car is already exists but price changed
                    self.exists_count += 1
                    car_id, current_price = car_exists

                    exists_name = self.get_car_name(car_id)
                    if exists_name:
                        exists_name_str = f"{exists_name['marka']} {exists_name['model']}"
                        if exists_name_str != name:
                            self.totally_changed_count += 1
                            continue

                    e_year, e_engine, e_used_by_km = self.get_existence_year_engine_used_by_km(car_id)
                    if car['year'] != e_year or car['engine'] != e_engine or car['used_by_km'] != e_used_by_km:
                        self.update_car_year_engine_used_by_km(car_id, car['year'], car['engine'], car['used_by_km'],
                                                               car['date'])
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

    def turbo_az_scraping_main(self):
        # pdb.set_trace()
        url = 'https://turbo.az/autos?page=1'  # starting point
        bs = self.get_beautiful_soup(url)
        items = self.get_items(bs)
        self.parse_items(items)
        # for cron log
        print(f"New: {self.new_count}\n"
              f"Old: {self.exists_count}\n"
              f"Price updated: {self.price_updated_count}\n"
              f"Details updated: {self.details_updated_count}\n"
              f"Totally changed: {self.totally_changed_count}")


if __name__ == '__main__':
    Turbo().turbo_az_scraping_main()
