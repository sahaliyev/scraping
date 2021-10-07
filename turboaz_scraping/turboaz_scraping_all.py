import traceback

import requests
from bs4 import BeautifulSoup

import re
from datetime import datetime
from user_agents import user_agents
import random
from turbo_db_operations import TurboDbOperations
from emoji import UNICODE_EMOJI

exclude_list = UNICODE_EMOJI.keys()
rx = "(?:{})+".format("|".join(map(re.escape, exclude_list)))


class Turbo(TurboDbOperations):
    def __init__(self):
        super(Turbo, self).__init__()
        self.base_url = 'https://turbo.az'
        self.count = 0

    @staticmethod
    def my_ip():
        url = 'http://jsonip.com'
        r = requests.get(url)
        ip = r.json()['ip']
        return ip

    @staticmethod
    def get_beautiful_soup(link):
        headers = {
            'user-agent': random.choice(user_agents),
            'Content-Type': 'application/json; charset=utf-8',
        }
        response = requests.get(link, headers=headers)
        if response.status_code == 200 and response.url != 'https://turbo.az/' and 'duplicate' not in response.url:
            response.encoding = "utf8"
            html = response.text
            bs = BeautifulSoup(html, 'lxml')
            return bs
        elif response.status_code == 200 and (response.url == 'https://turbo.az/' or 'duplicate' in response.url):
            return 301
        elif response.status_code == 503:
            return 503
        else:
            raise Exception(f'Request error. {response.status_code}')

    @staticmethod
    def parse_date(date):
        # '22 İyun 2012'
        months_dict = {'Yanvar': 1, 'Fevral': 2, 'Mart': 3, 'Aprel': 4, 'May': 5,
                       'İyun': 6, 'İyul': 7, 'Avqust': 8, 'Sentyabr': 9,
                       'Oktyabr': 10, 'Noyabr': 11, 'Dekabr': 12}

        day, month, year = date.split()
        try:
            month = months_dict[month]
        except KeyError:
            month = datetime.now().month
        return datetime.strptime(f"{day}.{month}.{year} 00:00", '%d.%m.%Y %H:%M')

    @staticmethod
    def modify_car(car):
        del car['ad_id']  # unique_id of car in turbo az
        car['ban_type'] = car.pop('Ban növü')
        car['year'] = int(car.pop('Buraxılış ili'))
        car['engine'] = car.pop('Mühərrik')
        car['engine_power'] = int(car.pop('Mühərrikin gücü').replace('a.g.', '').strip())
        car['color'] = car.pop('Rəng')
        car['gearbox'] = car.pop('Sürətlər qutusu')
        car['fuel_type'] = car.pop('Yanacaq növü')
        is_new = car.pop('Yeni')
        if is_new == 'Xeyr':
            car['new'] = 0
        else:
            car['new'] = 1
        car['used_by_km'] = int(car.pop('Yürüş').replace('km', '').replace(' ', ''))
        car['view_count'] = int(car.pop('ad_hits'))
        car['transmission'] = car.pop('Ötürücü')
        car['marka'] = car.pop('Marka')
        car['model'] = car.pop('Model')
        car['date'] = car.pop('ad_updated_at')
        car['city'] = car.pop('Şəhər')
        price_currency = car.pop('Qiymət')
        car['currency'] = price_currency.split()[-1]
        car['price'] = int(''.join(price_currency.split()[:-1]))
        return car

    @staticmethod
    def remove_emoji(car):
        car['description'] = re.sub(rx, u'', car.pop('description'))
        return car

    def new_or_existence_id(self, table_name, value):
        if table_name == 'seller':
            value = value.upper()  # this two lines makes seller name upper in order to prevent duplicates
        exists = self.item_exists(f'{table_name}', value)
        if exists:
            return exists[0]
        else:
            return self.insert_item_into_table(f'{table_name}', value)

    def parse_item_details(self, item_url):
        requests_count = 0
        while requests_count < 5:
            try:
                bs = self.get_beautiful_soup(item_url)
                if bs == 503:
                    return 'blocked'  # if cloudflare blocked ip stop process
                elif bs == 301:
                    return 'error'

                pending = bs.find('div', class_='pending-confirmation')
                if pending:
                    return 'error'

                car = {}
                container = bs.find('div', class_='product')

                # Below code takes images
                slider = container.find('div', class_='product-photos')
                images_list = []
                if slider:
                    images = slider.find_all('a')
                    for image in images:
                        if image.has_attr('href'):
                            src = image['href']
                            images_list.append(src)
                car['images'] = images_list  # the first image is main image

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

                # Barter
                barter = product_properties.find('li', class_='product-properties-i_barter')
                if barter:
                    car['barter'] = 1

                # Loan
                loan = product_properties.find('li', class_='product-properties-i_loan')
                if loan:
                    car['loan'] = 1

                # extra info
                extras = []
                product_extras = container.find('div', class_='product-extras')
                if product_extras:
                    items = product_extras.find_all('p', class_='product-extras-i')
                    for item in items:
                        value = item.text.strip()
                        extras.append(value)
                    car['details'] = extras

                # description
                text = ""
                desc = container.find('h2', class_='product-text')
                while desc:
                    desc = desc.find_next_sibling('p')
                    if desc:
                        text += desc.text.strip()
                car['description'] = text

                # archive data may not have seller name
                sold = container.find('div', class_='sold')
                if sold:
                    car['seller'] = 'Sold'
                    car['phone_numbers'] = list()  # no phone number available, returning empty list

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
                    car['seller'] = seller_name
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
                    car['seller'] = seller_name
                    car['phone_numbers'] = phones

                return car

            except Exception as e:
                info = {'link': item_url, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc())}
                self.insert_into_logs(info)
                requests_count += 1

    def parse_items(self, index):
        url_ = f"https://turbo.az/autos/{index}"
        try:
            car = self.parse_item_details(url_)
            if car == 'blocked':
                return 'blocked'  # exit from function if cloudflare blocked ip
            elif car == 'error':
                return
            car = self.modify_car(car)
            car = self.remove_emoji(car)
            car['date'] = self.parse_date(car.pop('date'))
            # if car['date'] < datetime.strptime('2020-01-01 00:00', '%Y-%m-%d %H:%M'):
            #     return
            car['is_parsed'] = 1
            car['is_filtered'] = 1
            car['unique_id'] = index
            car['url'] = url_

            fk_columns = ('ban_type', 'color', 'city', 'currency', 'fuel_type',
                          'gearbox', 'marka', 'model', 'transmission', 'seller')
            for fk_item in fk_columns:
                car[fk_item] = self.new_or_existence_id(fk_item, car.get(fk_item))

            inserted_id = self.insert_into_car(car)

            # insert inner images to table
            self.insert_into_images_archive(car['images'], inserted_id)

            # insert details into table
            for detail in car.get('details', []):
                unique_detail_id = self.new_or_existence_id('unique_details', detail)
                self.insert_into_details(unique_detail_id, inserted_id)

            # insert phone numbers into table
            for phone in car.get('phone_numbers', []):
                unique_phone_number_id = self.new_or_existence_id('unique_phone_number', phone)
                self.insert_into_phone_numbers(unique_phone_number_id, inserted_id)

        except Exception as e:
            info = {'link': url_, 'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc())}
            self.insert_into_logs(info)

        finally:
            current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
            with open('logs/archive.txt', 'a', encoding="utf-8") as file:
                print(f"{url_} at {current_time}", file=file)

    def turbo_az_scraping_main(self):
        starting_index = self.read_file()
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        log = f"Archive restarted from {starting_index} at {current_time}"
        with open('logs/restart_archive.txt', 'a', encoding="utf-8") as file:
            print(f"{log}\n", file=file)
        for index in range(starting_index, 4500000):
            if self.car_exists(index):
                continue
            is_blocked = self.parse_items(index)
            if is_blocked:
                break

    @staticmethod
    def read_file():
        file = open('/home/ferrumcapital/Sahil/ferrum_scraping/logs/archive.txt')
        # file = open('/home/ferrum/Sahil/ferrum_scraping/logs/archive.txt')
        for line in reversed(file.readlines()):
            return int(line.split()[0].split('/')[-1])

    def test(self):
        for i in (3677508,):
            self.parse_items(i)


if __name__ == '__main__':
    t = Turbo()
    t.turbo_az_scraping_main()
    # t.test()

