import pdb
import random
import time
import traceback

import pandas as pd
import requests
from bs4 import BeautifulSoup

from user_agents import user_agents


class BakuElectronicParser:
    def __init__(self):
        self.base_url = 'https://www.bakuelectronics.az'

    @staticmethod
    def get_bs(url):
        time.sleep(1.5)
        headers = {
            'User-Agent': random.choice(user_agents)
        }
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f'Request error. {response.status_code} {response.text}')
        response.encoding = "utf8"
        html = response.text

        bs = BeautifulSoup(html, 'lxml')
        return bs

    @staticmethod
    def get_popular_links(bs):
        container = bs.find('ul', class_='filters')
        items = container.find_all('li')
        return [item.a['href'] for item in items]

    @staticmethod
    def get_pagination(bs):
        container = bs.find('ul', class_='paging')
        items = container.find_all('li', class_='paging__item')
        return [x.a['href'] for x in items]

    def get_products(self, bs):
        container = bs.find('div', id='mse2_results')
        items = container.find_all('div', class_='catalog__col')

        product_list = list()
        for item in items:
            md = dict()

            title = item.find('a', class_='product__title')
            if title:
                md['title'] = title.get_text().strip()

            img = item.find('img')
            if img and img.has_attr('src'):
                md['image'] = self.base_url + img['src']

            url = item.find('a', class_='product__img-wrap')
            if url and url.has_attr('href'):
                md['url'] = url['href']
            product_list.append(md)
        return product_list

    def get_specification(self, url):
        md = dict()
        try:
            bs = self.get_bs(url)
            container = bs.find('ul', class_='specs-table__set')
            if container:
                items = container.find_all('li')
                for item in items:
                    key = item.find('span', class_='specs-table__spec')
                    value = item.find('span', class_='specs-table__text')
                    if key:
                        key = key.get_text().strip()
                    if value:
                        value = value.get_text().strip()

                    md[key] = value
                return md
        except Exception as e:
            print(e, str(traceback.format_exc()))
            pass

    @staticmethod
    def convert_to_df_and_save(data, category):
        df = pd.DataFrame(data)
        df.to_excel(f"output/{category}.xlsx", index=False)

    def main(self):
        main_url = 'https://www.bakuelectronics.az/catalog/telefonlar-qadcetler/'
        try:
            bs = self.get_bs(main_url)
            popular_links = self.get_popular_links(bs)
            # main_item_list = list()

            for pl in popular_links[5:]:
                category = pl.split('/')[-2]
                try:
                    bs = self.get_bs(pl)
                    items = self.get_products(bs)

                    pagination_links = self.get_pagination(bs)
                    for pag_l in pagination_links[1:]:
                        try:
                            bs = self.get_bs(pag_l)
                            items.extend(self.get_products(bs))
                        except Exception as e:
                            print(e)
                            pass

                    print(str(len(items)) + ' is going to be parsed! With ' + category)

                    for item in items:
                        if not item:
                            continue
                        # pdb.set_trace()
                        try:
                            item.update(self.get_specification(item['url']))
                            print(item['url'] + ' parsed!')
                            item['parsed'] = 1
                        except (KeyError, TypeError):
                            continue

                    self.convert_to_df_and_save(items, category)
                except Exception as e:
                    print(e, str(traceback.format_exc()))
                    pass
        except Exception as e:
            print(e, str(traceback.format_exc()))
            pass


if __name__ == '__main__':
    BakuElectronicParser().main()
