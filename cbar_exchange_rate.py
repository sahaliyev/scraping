from datetime import datetime

import requests
from bs4 import BeautifulSoup

from db_queries import DBQueries


class CbarExchangeRate(DBQueries):
    def __init__(self):
        super().__init__()

        self.currency_dict = {
            'USD': 2,
            'EUR': 3
        }

    def get_valute(self):
        now = datetime.now()
        day = '{:02d}'.format(now.day)
        month = '{:02d}'.format(now.month)
        url = f'https://www.cbar.az/currencies/{day}.{month}.{now.year}.xml'
        response = requests.get(url)
        if response.status_code == 200:
            valute_list = list()
            content = response.content
            soup = BeautifulSoup(content, "xml")
            for item in soup.find_all('Valute'):
                if item.has_attr('Code') and item['Code'] in ('USD', 'EUR'):
                    valute_dict = {'currency_id': self.currency_dict.get(item['Code']),
                                   'rate': item.Value.get_text()}
                    valute_list.append(valute_dict)
            return valute_list

    def main(self):
        items = self.get_valute()
        for item in items:
            self.update_exchange_rate(item['currency_id'], float(item['rate']))

        current_time = datetime.strftime(datetime.now(), '%d.%m.%Y')
        print(f"{items} at {current_time}")  # for cron log


if __name__ == '__main__':
    CbarExchangeRate().main()
