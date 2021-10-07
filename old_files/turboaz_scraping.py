import pdb
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
    """
    Older version of turbo.az scraping which does all the job.
    Because of data lost and having many functionality I separated this script into two parts.
    One is named turboaz_scraping_outer.py which is responsible for taking last item from the site.
    Second one is turboaz_scraping_inner.py which is responsible for parsing details of items that populated by turboaz_scraping_outer.py

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

    @staticmethod
    def modify_car(car):
        del car['ad_id']
        del car['ad_updated_at']

        car['ban_type'] = car.pop('Ban növü')
        car['year'] = car.pop('Buraxılış ili')
        car['engine'] = car.pop('Mühərrik')
        car['engine_power'] = car.pop('Mühərrikin gücü')
        car['color'] = car.pop('Rəng')
        car['gearbox'] = car.pop('Sürətlər qutusu')
        car['fuel_type'] = car.pop('Yanacaq növü')
        car['new'] = car.pop('Yeni')
        car['used_by_km'] = car.pop('Yürüş')
        car['view_count'] = car.pop('ad_hits')
        car['transmission'] = car.pop('Ötürücü')
        car['marka'] = car.pop('Marka')
        car['model'] = car.pop('Model')
        return car

    @staticmethod
    def remove_emoji(car):
        car['description'] = re.sub(rx, u'', car.pop('description'))
        return car

    def parse_item_details(self, url):
        try:
            bs = self.get_beautiful_soup(url)
            car = {}
            container = bs.find('div', class_='product')

            # Below code takes images
            slider = container.find('div', class_='product-photos')
            images_list = []
            if slider:
                images_container = slider.find('div', class_='product-photos-thumbnails')
                images = images_container.find_all('a')
                for image in images:
                    src = image['href']
                    images_list.append(src)
            car['inner_images'] = images_list

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
                    ignore_list = ['Qiymət', 'Şəhər']
                    name = name.text.strip()
                    if name in ignore_list:
                        continue
                    value = item.find('div', class_='product-properties-value').text.strip()
                    car[name] = value

            # Barter
            barter = product_properties.find('li', class_='product-properties-i_barter')
            if barter:
                car['barter'] = 'Yes'

            # Loan
            loan = product_properties.find('li', class_='product-properties-i_loan')
            if loan:
                car['loan'] = 'Yes'

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
                car['seller_name'] = seller_name
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
                car['seller_name'] = seller_name
                car['phone_numbers'] = phones

            return car

        except Exception as e:
            info = {'exception_info': f"{type(e)}:{e.args}",
                    'traceback_info': str(traceback.format_exc()),
                    'date': datetime.now()}
            with open('turbo_errors.txt', 'a') as file:
                print(info, file=file)

    def parse_items(self, items):
        all_cars = []
        for item in items:
            car = {}
            try:
                url = item.a['href']
                item_id = url.replace('/autos/', '').split('-')[0]
                car['unique_id'] = int(item_id)
                if self.car_exists(car['unique_id']):
                    continue

                car['url'] = self.base_url + url
                outer_image = item.find('img')
                if outer_image:
                    car['main_image'] = outer_image['src']
                price_container = item.find('div', class_='product-price')
                currency_span = price_container.find('span')
                car['currency'] = currency_span.text.strip()
                currency_span.decompose()
                car['price'] = price_container.text.strip()
                car['name'] = item.find('p', class_='products-name').text.strip()
                place_date = item.find('div', class_='products-bottom').text.strip()
                place, date = place_date.split(',')  # date still has empty space at the beginning
                car['city'] = place
                car['date'] = datetime.strptime(date.strip(), '%d.%m.%Y %H:%M')  # this solves that problem

                car.update(self.parse_item_details(car['url']))
                car = self.modify_car(car)
                car = self.remove_emoji(car)
                all_cars.append(car)  # append car to all cars to use later for sending email
                last_inserted_car_id = self.insert_into_car(car)
                self.count += 1
                self.insert_into_images(car['main_image'], car['inner_images'], last_inserted_car_id)
                self.insert_into_details(car['details'], last_inserted_car_id)
                self.insert_into_phone_numbers(car['phone_numbers'], last_inserted_car_id)

            except Exception as e:
                link = car.get('url', 'N/A')
                info = {'link': link, 'exception_info': f"{type(e)}:{e.args}",
                        'traceback_info': str(traceback.format_exc()),
                        'date': datetime.now()}
                with open('turbo_errors.txt', 'a') as file:
                    print(info, file=file)

        return all_cars

    def turboaz_scraping_main(self):
        pdb.set_trace()
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        url = 'https://turbo.az/autos?page=1'  # starting point
        bs = self.get_beautiful_soup(url)
        items = self.get_items(bs)
        all_new_cars = self.parse_items(items)
        print(f"# of new items: {self.count} at {current_time}")  # for cron log
        # requirements = self.get_requirements()
        # filtered_df = self.filtered_dfs(all_new_cars, requirements)
        # print(f"# of desired items: {len(filtered_df.index)} at {current_time}")  # for cron log
        # self.send_mail_main(filtered_df)


if __name__ == '__main__':
    Turbo().turboaz_scraping_main()
