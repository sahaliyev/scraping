import math
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

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class AutoAz(DBQueries):
    def __init__(self):
        super(AutoAz, self).__init__()
        self.base_url = 'http://www.auto.az/'
        self.source_id = 2  # to distinguish turbo and auto cars in table
        self.new_count = 0
        self.exists_count = 0
        self.updated_count = 0

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
        container = bs.find('ul', class_='cat_ul2')
        if container:
            items = container.find_all('li')
            return items

    def parse_items(self, items):
        for item in items:
            car = dict()
            try:
                url = item.a['href']
                pattern = r'\d+'
                item_id = re.search(pattern, url)
                if item_id:
                    item_id = item_id.group()
                    car['unique_id'] = int(item_id)
                    price = item.find('span', class_='b').get_text().strip()
                    currency, price = price.split()
                    currency_id = self.new_or_existence_id('currency', currency)
                    rate = self.get_currency_rate(currency_id)[0]
                    car['price'] = math.ceil(int(price) * rate)

                    car_exists = self.car_exists(self.source_id, car['unique_id'])
                    if car_exists:
                        car_id, price = car_exists
                        if car['price'] != price:
                            self.mark_car_as_unparsed_unfiltered(car_id, is_price_updated=True)
                        self.exists_count += 1
                        continue

                car['url'] = self.base_url + url
                car['source_id'] = self.source_id

                self.insert_into_car_partial(self.source_id, car)
                self.new_count += 1

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

    def auto_az_scraping_main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        url = self.base_url  # starting point
        bs = self.get_beautiful_soup(url)
        items = self.get_items(bs)
        self.parse_items(items)
        # for cron log
        print(f"New: {self.new_count}, Old {self.exists_count} ({self.updated_count} updated) items at {current_time}")


if __name__ == '__main__':
    AutoAz().auto_az_scraping_main()
