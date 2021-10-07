import pdb
import sys
import time

sys.path.append('../')

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from datetime import datetime
from sys import platform
from pprint import pprint as pp


class LinkedinParser:
    def __init__(self):
        super(LinkedinParser, self).__init__()

    @staticmethod
    def initialize_driver():
        options = Options()

        # for ubuntu
        if platform == 'linux':
            options.headless = True
            driver = webdriver.Firefox(options=options)
        else:  # for windows
            path = r"C:\Users\sahil.aliyev\geckodriver.exe"
            driver = webdriver.Firefox(options=None, executable_path=path)
        return driver

    @staticmethod
    def login(driver):
        driver.get('https://www.linkedin.com/login?fromSignIn=true&trk=guest_homepage-basic_nav-header-signin')
        username = driver.find_element_by_id('username')
        password = driver.find_element_by_id('password')
        if username:
            username.clear()
            username.send_keys('sahil.eyev@gmail.com')
            # username.send_keys(Keys.RETURN)
        if password:
            password.clear()
            password.send_keys('s7514596')
            # password.send_keys(Keys.RETURN)
        login_button = driver.find_element_by_class_name('btn__primary--large')
        if login_button:
            login_button.click()

    def parse_item(self, driver, url):
        try:
            md = dict()
            driver.get(url)
            time.sleep(7)
            container = driver.find_element_by_xpath('//main//div[2]//div//div[2]//div[1]//section')
            items = container.find_elements_by_class_name('org-page-details__definition-term')
            items = [item.text for item in items if item.text != 'Company size']
            values = container.find_elements_by_class_name('org-page-details__definition-text')
            for index, item in enumerate(zip(items, values)):
                key, value = item
                md[key] = value.text.strip().replace('\n', ' ')

            md['Overview'] = container.find_element_by_class_name('break-words').text.strip().replace('\n', ' ')

            comp_size1 = container.find_element_by_class_name('org-about-company-module__company-size-definition-text')
            comp_size2 = container.find_element_by_class_name('org-page-details__employees-on-linkedin-count')
            # print(comp_size1, comp_size1.text)
            md['Company size'] = f"{comp_size1.text}. {comp_size2.text}".replace('\n', '. ')
            md['url'] = url
            pp(md)

            """if container:
                container = container.text
                try:
                    splitted_by_website = container.split('Website')
                    md['overview'] = splitted_by_website[0].replace('Overview', '').strip().replace('\n', '')
                except Exception as e:
                    pass

                try:
                    splitted_by_industry = splitted_by_website[1].split('Industry')
                    md['website'] = splitted_by_industry[0].strip().replace('\n', '')
                except Exception as e:
                    pass
                try:
                    splitted_by_company_size = splitted_by_industry[1].split('Company size')
                    md['industry'] = splitted_by_company_size[0].strip().replace('\n', '')
                except Exception as e:
                    pass
                try:
                    splitted_by_headquarters = splitted_by_company_size[1].split('Headquarters')
                    md['company_size'] = splitted_by_headquarters[0].strip().replace('\n', '. ')
                except Exception as e:
                    pass
                try:
                    splitted_by_type = splitted_by_headquarters[1].split('Type')
                    md['headquarters'] = splitted_by_type[0].strip()
                except Exception as e:
                    pass
                try:
                    splitted_by_founded = splitted_by_type[1].split('Founded')
                    md['type'] = splitted_by_founded[0].strip()
                except Exception as e:
                    pass
                try:
                    founded, specialties = splitted_by_founded[1].split('Specialties')
                    md['founded'] = founded.strip()
                    md['specialties'] = specialties.strip()
                except Exception as e:
                    pass
            return md"""
        except Exception as e:
            print(e)

    def main(self):
        items = list()
        driver = self.initialize_driver()
        self.login(driver)
        pages = ['https://www.linkedin.com/company/ferrum-capital-mmc/about/',
                 'https://www.linkedin.com/company/labrin/about/']
        for page in pages:
            try:
                single_page_detail = self.parse_item(driver, page)
                # print(list(single_page_detail.values()))
                # with open('linkedin_company_infos.csv', 'a') as fd:
                #     fd.write(', '.join(list(single_page_detail.values())))
                items.append(single_page_detail)
            except Exception as e:
                continue
        pp(items)


if __name__ == '__main__':
    e = LinkedinParser()
    e.main()
