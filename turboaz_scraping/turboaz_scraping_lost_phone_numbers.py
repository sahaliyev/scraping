import traceback

import requests
from bs4 import BeautifulSoup

import re
from user_agents import user_agents
import random
from filter_and_send_mail import FilterAndSendMail
from emoji import UNICODE_EMOJI

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class Turbo(FilterAndSendMail):
    """
    some cars in table has is_parsed=2 means it parsed before but unable to parse right now.
    Also it is not sure if they have phone numbers.
    """
    def __init__(self):
        super(Turbo, self).__init__()
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
            container = bs.find('div', class_='product')

            # seller contact (person)
            seller = container.find('div', class_='seller-contacts')
            if seller:
                seller_name = seller.find('div', class_='seller-name').text.strip()
                phones = []
                phones_container = container.find('div', class_='seller-phone')
                items = phones_container.find_all('a', class_='phone')
                for item in items:
                    phone = item.text.strip()
                    phones.append(phone)
                car['phone_numbers'] = phones
            # seller contact (shop)
            seller_shop = container.find('div', class_='shop-contact')
            if seller_shop:
                seller_name = seller_shop.find('a', class_='shop-contact--shop-name').text.strip()
                phones = []
                items = seller_shop.find_all('div', class_='shop-contact--phones-i')
                for item in items:
                    item = item.find('a', class_='shop-contact--phones-number')
                    phone = item.text.strip()
                    phones.append(phone)
                car['phone_numbers'] = phones

            return car

        except Exception as e:
            info = {'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc())}
            self.insert_into_logs(info)

    def parse_items(self, items):
        for car in items:
            car_id = car.get('id')
            try:
                car = dict(car)  # in order to be able to update, I convert db row to dict
                car.update(self.parse_item_details(car['url']))

                # insert phone numbers into table
                for phone in car.get('phone_numbers', []):
                    unique_phone_number_id = self.new_or_existence_id('unique_phone_number', phone)
                    self.insert_into_phone_numbers(unique_phone_number_id, car_id)

                # mark as parsed
                self.update_car_lost_phone_numbers(car_id)

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

    def turbo_az_scraping_main(self):
        # pdb.set_trace()
        items = self.get_unparsed_items()
        self.parse_items(items)


if __name__ == '__main__':
    Turbo().turbo_az_scraping_main()
