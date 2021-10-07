import math
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
import utils
import source

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class TapAz(DBQueries):
    def __init__(self):
        super(TapAz, self).__init__()
        self.base_url = 'https://tap.az'
        self.source_id = source.TAPAZ_SITE_ID
        self.new_count = 0
        self.exists_count = 0
        self.price_updated_count = 0

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
        container = bs.find('div', class_='categories-products')
        items_container = container.find('div', class_='endless-products')
        items = items_container.find_all('div', class_='products-i')
        return items

    @staticmethod
    def parse_date_time(value):
        #  Bakı, 19 dekabr 2020
        #  Bakı, dünən, 02:42
        #  Şamaxı, bugün, 00:12
        value = value.replace(',', '')

        try:
            city, today_yesterday, time = value.split()
            if today_yesterday == 'dünən':
                date = (datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')
                full_date = datetime.strptime(f"{date} {time}", '%d.%m.%Y %H:%M')
                return city, full_date
            else:
                date = datetime.now().strftime('%d.%m.%Y')
                full_date = datetime.strptime(f"{date} {time}", '%d.%m.%Y %H:%M')
                return city, full_date
        except ValueError:
            city, day, month, year = value.split()
            try:
                day = utils.month_dict_for_tapaz[day]
            except KeyError:
                day = datetime.now().month
            full_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y')
            return city, full_date

        pass

    def parse_items(self, items):
        for item in items:
            car = dict()
            try:
                url = item.find('a', class_='products-link')['href']
                car['url'] = self.base_url + url
                car['source_id'] = self.source_id

                item_id = url.split('/')[-1]
                car['unique_id'] = int(item_id)

                currency = item.find('span', class_='price-cur').text.strip()
                currency_id = self.new_or_existence_id('currency', currency)
                rate = self.get_currency_rate(currency_id)[0]
                price = item.find('span', class_='price-val').text.strip()
                price = int(price.replace(' ', ''))
                car['price'] = math.ceil(price * rate)

                place_date = item.find('div', class_='products-created').text.strip()
                city, date = self.parse_date_time(place_date)
                car['city'] = self.new_or_existence_id('city', city)
                car['date'] = date

                car_exists = self.car_exists(self.source_id, car['unique_id'])
                if car_exists:
                    # increment exists_count if car is already exists but price changed
                    self.exists_count += 1
                    car_id, current_price = car_exists

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

    def tap_az_scraping_main(self):
        url = 'https://tap.az/elanlar/neqliyyat/avtomobiller'  # starting point
        bs = self.get_beautiful_soup(url)
        items = self.get_items(bs)
        self.parse_items(items)
        # for cron log
        print(f"New: {self.new_count}\n"
              f"Old: {self.exists_count}\n"
              f"Price updated: {self.price_updated_count}")


if __name__ == '__main__':
    TapAz().tap_az_scraping_main()
