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
import utils
import source

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class Rover(DBQueries):
    def __init__(self):
        super(Rover, self).__init__()
        self.source_id = source.ROVERAZ_SITE_ID
        self.base_url = 'https://rover.az'
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
        car['ban_type'] = car.pop('Ban növü', None)
        car['color'] = car.pop('Rəng', None)
        car['gearbox'] = car.pop('Sürət qutusu', None)
        car['fuel_type'] = car.pop('Yanacaq növü', None)
        car['transmission'] = car.pop('Ötürücü', None)
        return car

    @staticmethod
    def remove_emoji(description):
        if description:
            return re.sub(rx, u'', description)

    def parse_item_details(self, url):
        try:
            bs = self.get_beautiful_soup(url)
            car = dict()
            container = bs.find('div', class_='mt-5')

            # Below code takes images
            slider = container.find('div', class_='photoswipe')
            images_list = list()
            if slider:
                images = slider.find_all('a')
                for image in images:
                    src = self.base_url + image['href']
                    images_list.append(src)
            car['images'] = images_list

            # Below code takes statistics such as view count, update time and product id
            statistics = container.find('div', class_='pt-2')
            items = statistics.find_all('div', class_='col-lg-3')
            for item in items:
                label, value = item.find_all('div')
                if label and value:
                    car[label.get_text().strip()] = value.get_text().strip()

            extras = list()

            details_container, description_container = container.find_all('fieldset', class_='mt-4')
            if details_container:
                items = details_container.find_all('div', class_='col-md-6')
                for item in items:
                    extras.append(item.get_text().strip())
            car['details'] = extras

            # description
            car['description'] = None
            if description_container:
                desc = description_container.find('div', class_='col-md-12')
                if desc:
                    car['description'] = desc.get_text().strip()

            # seller contact (person)
            seller = container.find('div', class_='mt-2')
            if seller:
                seller_name = seller.find('div', class_='mb-1')
                if seller_name:
                    car['seller'] = seller_name.get_text().strip()

                phones_container = seller.find('div', class_='mb-2')
                if phones_container:
                    car['phone_number'] = phones_container.get_text().strip()

            view_count = container.find('div', class_='float-right')
            if view_count:
                car['view_count'] = int(view_count.get_text().strip())

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

                ban_type = car['ban_type'].lower()
                if ban_type in ('kvadrosikl', 'skuter'):
                    continue
                try:
                    car['ban_type'] = utils.ban_types_dict[ban_type]
                except KeyError:
                    car['ban_type'] = ban_type

                fk_columns = ('ban_type', 'color', 'fuel_type', 'gearbox', 'transmission', 'seller')
                for fk_item in fk_columns:
                    car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

                self.update_car(self.source_id, car)  # update item in database
                self.count += 1
                # insert inner images to table
                self.insert_into_images(car['images'], car_id)

                # insert details into table
                details = car.get('details')
                if details:
                    if 'Ön park radarı' in details and 'Arxa park radarı' in details:
                        details.remove('Ön park radarı')
                    for detail in details:
                        detail = detail.lower()
                        try:
                            detail = utils.details_dict[detail]
                        except KeyError:
                            detail = detail
                        unique_detail_id = self.new_or_existence_id('unique_details', detail)
                        self.insert_into_details(unique_detail_id, car_id)

                # insert phone numbers into table
                if car.get('phone_number'):
                    unique_phone_number_id = self.new_or_existence_id('unique_phone_number', car['phone_number'])
                    self.insert_into_phone_numbers(unique_phone_number_id, car_id)

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)

            finally:
                self.increment_request_count(car_id, request_count)

    def rover_az_scraping_main(self):
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        items = self.get_unparsed_items(self.source_id)
        self.parse_items(items)
        print(f"Parsed: {self.count} items at {current_time}")  # for cron log
        print('-' * 50)

    # the same details for radar can be added consider that.


if __name__ == '__main__':
    Rover().rover_az_scraping_main()
