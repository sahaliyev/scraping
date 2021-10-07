import pdb
import traceback

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from user_agents import user_agents
import random
from emoji import UNICODE_EMOJI
from auto_db_operations import AutoDbOperations
exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class AutoAz(AutoDbOperations):
    def __init__(self):
        super(AutoAz, self).__init__()
        self.base_url = 'http://www.auto.az'
        self.source_id = 2  # to distinguish turbo and auto cars in table

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
        container = bs.find('ul', class_='car-block')
        if container:
            items = container.find_all('li')
            return items

    def new_or_existence_id(self, table_name, value):
        exists = self.item_exists(f'{table_name}', value)
        if exists:
            return exists[0]
        else:
            return self.insert_item_into_table(f'{table_name}', value)

    def parse_items(self, items):
        for item in items:
            car = {}
            try:
                url = item.a['href']
                item_id = url.replace('/car/', '').split('-')[0]
                car['unique_id'] = int(item_id)
                car_exists = self.auto_exists(car['unique_id'], 2)
                if car_exists:
                    continue

                info = item.find('div', class_='info2')
                car['year'] = int(info.find('span', class_='y').get_text().strip())
                car['engine'] = info.find('span', class_='m').get_text().strip()
                used_by_km = info.find('span', class_='r').get_text().strip()
                used_by_km = used_by_km.replace(' km', '')
                car['used_by_km'] = int(used_by_km)
                price = info.find('span', class_='p').get_text().strip()
                currency, price = price.split()
                car['currency'] = self.new_or_existence_id('currency', currency)
                car['price'] = int(price)

                car['url'] = self.base_url + url
                car['source_id'] = self.source_id

                loan = item.find('div', class_='info3')
                if loan:
                    car['loan'] = 1
                else:
                    car['loan'] = 0
                if car['used_by_km'] == 0:
                    car['new'] = 1
                self.insert_into_car_partial_auto(car)

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

    def auto_az_scraping_main(self):
        # current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        for i in range(1, 37):
            url = f'http://www.auto.az/cars/page/{i}'  # starting point
            print(url)
            bs = self.get_beautiful_soup(url)
            items = self.get_items(bs)
            self.parse_items(items)
            # print(f"New: {self.new_count}, Old {self.exists_count} items at {current_time}")  # for cron log


if __name__ == '__main__':
    AutoAz().auto_az_scraping_main()
