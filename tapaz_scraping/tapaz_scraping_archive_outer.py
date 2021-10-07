import sys

sys.path.append('../')

import pdb
import traceback

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from user_agents import user_agents
import random
from tapaz_db_operations import TapAzDbOperations
from emoji import UNICODE_EMOJI

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))
import pprint as pp


class TapAz(TapAzDbOperations):
    def __init__(self):
        super(TapAz, self).__init__()
        self.base_url = 'https://tap.az'
        self.source_id = 3

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
        pagination = items_container.find('div', class_='pagination')
        next_page = pagination.find('a')['href']
        return items, next_page

    def new_or_existence_id(self, table_name, value):
        exists = self.item_exists(f'{table_name}', value)
        if exists:
            return exists[0]
        else:
            return self.insert_item_into_table(f'{table_name}', value)

    @staticmethod
    def parse_date_time(value):
        month_dict = {'yanvar': '01', 'fevral': '02', 'mart': '03',
                      'aprel': '04', 'may': '05', 'iyun': '06',
                      'iyul': '07', 'avqust': '08', 'sentyabr': '09',
                      'oktyabr': '10', 'noyabr': '11', 'dekabr': '12'
                      }

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
                day = month_dict[day]
            except KeyError:
                day = datetime.now().month
            full_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y')
            return city, full_date

        pass

    def parse_items(self, items):
        exists_count = new_count = 0
        for item in items:
            car = {}
            try:
                url = item.find('a', class_='products-link')['href']

                item_id = url.split('/')[-1]
                car['unique_id'] = int(item_id)

                currency = item.find('span', class_='price-cur').text.strip()
                car['currency'] = self.new_or_existence_id('currency', currency)
                price = item.find('span', class_='price-val').text.strip()
                car['price'] = int(price.replace(' ', ''))

                place_date = item.find('div', class_='products-created').text.strip()
                city, date = self.parse_date_time(place_date)
                car['city'] = self.new_or_existence_id('city', city)
                car['date'] = date

                car_exists = self.tap_az_exists(car['unique_id'], 3)
                if car_exists:
                    exists_count += 1
                    continue

                car['url'] = self.base_url + url
                car['source_id'] = self.source_id
                self.insert_into_car_partial_tap_az(car)
                # pp.pprint(car)
                new_count += 1  # for logging

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

        return new_count, exists_count

    @staticmethod
    def read_file():
        try:
            file = open('tapaz_archive.txt')
            for line in reversed(file.readlines()):
                return line
        except OSError:
            print("File does not exists!")
            return 'https://tap.az/elanlar/neqliyyat/avtomobiller'

    def tap_az_scraping_main(self):
        url = self.read_file()
        while url:
            print(url)
            with open('tapaz_archive.txt', 'a', encoding="utf-8") as file:
                print(f"{url}", file=file)
            bs = self.get_beautiful_soup(url)
            items, url_ = self.get_items(bs)
            url_ = self.base_url + url_
            new_count, exists_count = self.parse_items(items)
            with open('tapaz_archive_info.txt', 'a', encoding="utf-8") as file:
                print(f"New {new_count} Old {exists_count} from {url}", file=file)
            url = url_


if __name__ == '__main__':
    TapAz().tap_az_scraping_main()
