import sys

sys.path.append('../')

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from db_queries import DBQueries
from datetime import datetime


class ExchangeRate(DBQueries):
    def __init__(self):
        super(ExchangeRate, self).__init__()

    @staticmethod
    def get_selenium(link):
        options = Options()
        # options.headless = True

        # for windows
        path = r"C:\Users\sahil.aliyev\geckodriver.exe"
        driver = webdriver.Firefox(options=options, executable_path=path)

        # for ubuntu
        # driver = webdriver.Firefox(options=options)

        driver.get(link)
        page_source = driver.page_source
        driver.quit()
        return page_source

    @staticmethod
    def parse_xe_com(page_source):
        soup = BeautifulSoup(page_source, 'lxml')
        inverse_row = soup.find('tr', class_='inverseRow')
        rates = inverse_row.find_all('td', class_='rateCell')

        rates_dict = dict()
        for rate in rates:
            a = rate.find('a')
            if a and a.has_attr('href'):
                href = a['href']
                wanted_part = href.split('from=')[1]
                currency = wanted_part.split('&')[0]
                currency_rate = a.get_text().strip()
                rates_dict[currency] = currency_rate
        return rates_dict

    def update_rates(self, rates_dict):
        usd = rates_dict.get('USD')
        eur = rates_dict.get('EUR')
        if usd:
            usd = float(usd)
            self.update_exchange_rate(2, usd)
        if eur:
            eur = float(eur)
            self.update_exchange_rate(3, eur)

    def exchange_rate_main(self):
        url = 'https://www.xe.com/currency/azn-azerbaijan-manat'
        page_source = self.get_selenium(url)
        if page_source:
            rates = self.parse_xe_com(page_source)
            self.update_rates(rates)
            current_time = datetime.strftime(datetime.now(), '%d.%m.%Y %H:%M')
            # with open('logs/exchange_rates.txt', 'a', encoding="utf-8") as file:
            #     print(f"{rates} at {current_time}", file=file)
            print(f"{rates} at {current_time}")  # for cron log


if __name__ == '__main__':
    e = ExchangeRate()
    e.exchange_rate_main()
