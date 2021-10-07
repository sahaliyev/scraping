import pdb

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
    def get_pagination_count(bs):
        container = bs.find('div', class_='page-content')
        if container:
            pagination = container.find('div', class_='pagination-container')
            if pagination:
                pagination = pagination.find('nav', class_='pagination')
                if pagination:
                    last = pagination.find('span', class_='last')
                    if last:
                        a = last.find('a')
                        if a and a.has_attr('href'):
                            value = a['href']  # /autos?page=417
                            return int(value.replace('/autos?page=', ''))

    def get_last_page_items_count(self, index):
        url_ = f'https://turbo.az/autos?page={index}'
        bs = self.get_beautiful_soup(url_)
        titles = bs.find_all('p', class_='section-title_name')
        title_parent = None
        for title in titles:
            if title.text.strip() == 'ELANLAR':
                title_parent = title.parent
        if title_parent:
            items_container = title_parent.find_next_sibling('div', class_='products')
            items = items_container.find_all('div', class_='products-i')
            return len(items)

    def turbo_az_scraping_main(self):
        pdb.set_trace()
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
        url = 'https://turbo.az/autos?page=1'  # starting point
        bs = self.get_beautiful_soup(url)
        last_page_number = self.get_pagination_count(bs)
        if last_page_number:
            total_count = (last_page_number - 1) * 24
            total_count += self.get_last_page_items_count(last_page_number)
            print(f"# of car in turbo.az: {total_count} at {current_time}")  # for cron log


if __name__ == '__main__':
    Turbo().turbo_az_scraping_main()
