import pdb
import sys

sys.path.append('../')

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from db_queries import DBQueries
from selenium.webdriver.support.ui import Select
from datetime import datetime
from sys import platform


class AllMarkaModel(DBQueries): # inherit
    def __init__(self):
        super(AllMarkaModel, self).__init__()
        self.marka_count = 0
        self.model_count = 0

    def parse_items(self):
        # pdb.set_trace()
        current_time = datetime.strftime(datetime.now(), "%d.%m.%Y")

        options = Options()

        # for ubuntu
        if platform == 'linux':
            options.headless = True
            driver = webdriver.Firefox(options=options)
        else:  # for windows
            path = r"C:\Users\sahil.aliyev\geckodriver.exe"
            driver = webdriver.Firefox(options=None, executable_path=path)

        driver.get('https://turbo.az/autos/new')
        elem = driver.find_element_by_class_name('limits--phone-number')
        if elem:
            elem.clear()
            elem.send_keys('0517514596')
            elem.send_keys(Keys.RETURN)
            marka = driver.find_element_by_id('auto_make_id')
            if marka:
                marka_list = marka.find_elements_by_tag_name("option")
                self.marka_count = len(marka_list)
                for x in marka_list:
                    text = x.text
                    value = x.get_attribute("value")
                    if text == 'Seçin':
                        continue

                    select = Select(driver.find_element_by_id('auto_make_id'))
                    select.select_by_value(value)
                    models = driver.find_element_by_id('auto_model_id')
                    models_list = models.find_elements_by_tag_name("option")
                    models_list_values = [x.text for x in models_list if x.text != 'Seçin']
                    self.model_count += len(models_list_values)

                    marka_id = self.new_or_existence_id('marka', text)
                    for item in models_list_values:
                        self.new_or_existence_model(marka_id, item)
            print(f"Marka count: {self.marka_count}, Model count: {self.model_count} at {current_time}")  # for cron log
            driver.close()


if __name__ == '__main__':
    e = AllMarkaModel()
    e.parse_items()
