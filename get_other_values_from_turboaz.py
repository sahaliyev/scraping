import sys

sys.path.append('../')

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.keys import Keys
from db_queries import DBQueries
from sys import platform


class GetOtherValuesFromTurbo(DBQueries):
    def __init__(self):
        super(GetOtherValuesFromTurbo, self).__init__()
        self.marka_count = 0
        self.model_count = 0

    def parse_items(self):
        options = Options()

        # for ubuntu
        if platform == 'linux':
            options.headless = True
            driver = webdriver.Firefox(options=options)
        else:  # for windows
            path = r"C:\Users\geckodriver.exe"
            driver = webdriver.Firefox(options=None, executable_path=path)

        driver.get('https://turbo.az/autos/new')

        elem = driver.find_element_by_class_name('limits--phone-number')
        if elem:
            elem.clear()
            elem.send_keys('0517514596')
            elem.send_keys(Keys.RETURN)

            drop_down_ids = {'auto_category_id': 'ban_type_new', 'auto_color_id': 'color_new',
                             'auto_fuel_type_id': 'fuel_type_new',
                             'auto_gear_id': 'transmission_new', 'auto_transmission_id': 'gearbox_new',
                             'auto_reg_year': 'year',
                             'auto_engine_volume': 'engine', 'auto_region_id': 'city_new'}

            for key, value in drop_down_ids.items():
                drop_down = driver.find_element_by_id(key)
                if drop_down:
                    drop_down_list = drop_down.find_elements_by_tag_name("option")
                    for x in drop_down_list:
                        text = x.text
                        if text == '':
                            continue

                        if value == 'engine':
                            text = int(text) / 1000
                        # write to database
                        self.new_or_existence_id(value, text)
            # unique details values
            extras = driver.find_element_by_class_name('auto_extras')
            if extras:
                items = extras.find_elements_by_class_name('checkbox')
                for item in items:
                    text = item.text
                    self.new_or_existence_id('unique_details_new', text)
            driver.close()


if __name__ == '__main__':
    e = GetOtherValuesFromTurbo()
    e.parse_items()
