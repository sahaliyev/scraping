import sys

sys.path.append('../')
from db_queries import DBQueries


class FilterCars(DBQueries):
    def __init__(self):
        super().__init__()

    @staticmethod
    def check_based_on_year(req, car_year):
        if not car_year:
            return False

        min_year = req.get('min_year')
        max_year = req.get('max_year')
        if min_year and max_year:
            return min_year <= car_year <= max_year
        elif min_year:
            return car_year >= min_year
        elif max_year:
            return car_year <= max_year
        # year did not supplied, so take all cars
        return True

    @staticmethod
    def check_based_on_price(req, car_price):
        if not car_price:
            return False

        min_price = req.get('min_price')
        max_price = req.get('max_price')
        if min_price and max_price:
            return min_price <= car_price <= max_price
        elif min_price:
            return car_price >= min_price
        elif max_price:
            return car_price <= max_price
        # no price supplied, so take all cars
        return True

    @staticmethod
    def check_based_on_used_by_km(req, car_used_by_km):
        if not car_used_by_km:
            return False

        req_used_by_km = req.get('used_by_km')
        if not req_used_by_km:  # no used_by_km specified, we can take all cars
            return True
        elif car_used_by_km <= req_used_by_km:
            return True
        return False

    def check_based_on_engine(self, req, car_engine):
        if not car_engine:
            return False

        req_id = req.get('id')
        if req_id:
            req_engines = self.get_part_of_requirement('requirements_engine', req_id)
            if not req_engines:
                return True
            else:
                for req_engine in req_engines:
                    engine_id = req_engine.get('engine_id')
                    if engine_id:
                        engine_value = self.get_requirement_value('engine', engine_id)
                        if engine_value:
                            engine_value = engine_value.get('engine')
                            if engine_value:
                                if str(engine_value) == str(car_engine):
                                    return False
                                return True

    '''def check_based_on_gearbox(self, req, car_gearbox):
        if not car_gearbox:
            return False

        req_id = req.get('id')
        if req_id:
            req_gearbox = self.get_part_of_requirement('requirements_gearbox', req_id)
            if req_gearbox:
                req_gearbox_ids = [x['gearbox_id'] for x in req_gearbox]
                if car_gearbox in req_gearbox_ids:
                    return True
                return False
            return True
        return True

    def check_based_on_ban_type(self, req, car_ban_type):
        if not car_ban_type:
            return False

        req_id = req.get('id')
        if req_id:
            req_ban_type = self.get_part_of_requirement('requirements_ban_type', req_id)
            if req_ban_type:
                req_ban_type_ids = [x['ban_type_id'] for x in req_ban_type]
                if car_ban_type in req_ban_type_ids:
                    return True
                return False
            return True
        return True'''

    def check_based_on_fuel_type(self, req, car_fuel_type):
        if not car_fuel_type:
            return False

        req_id = req.get('id')
        if req_id:
            req_fuel_types = self.get_part_of_requirement('requirements_fuel_type', req_id)
            if not req_fuel_types:
                return True
            else:
                for req_fuel_type in req_fuel_types:
                    fuel_type_id = req_fuel_type.get('fuel_type_id')
                    if fuel_type_id:
                        if int(fuel_type_id) == int(car_fuel_type):
                            return True
                        return False

    '''def check_based_on_details(self, req, car_details):
        if not car_details:
            return False

        req_id = req.get('id')
        if req_id:
            car_details_ids = {x['details'] for x in car_details}  # convert to set to check contains
            req_details = self.get_part_of_requirement('requirements_details', req_id)
            # convert to set to check is subset of car_details_ids
            if req_details:
                req_details_ids = {x['details_id'] for x in req_details}
                if req_details_ids.issubset(car_details_ids):
                    return True
                return False
            return True
        return True'''

    def check_single_car(self, car):
        mm = car.get('mm')
        year = car.get('year')
        price = car.get('price')
        engine = car.get('engine')
        fuel_type = car.get('fuel_type')
        used_by_km = car.get('used_by_km')

        if not mm:
            return False

        requirements = self.get_corresponding_requirements(mm)  # single requirement
        for req in requirements:
            if self.check_based_on_year(req, year):
                if self.check_based_on_price(req, price):
                    if self.check_based_on_used_by_km(req, used_by_km):
                        if self.check_based_on_engine(req, engine):
                            if self.check_based_on_fuel_type(req, fuel_type):
                                return True

    def filter_cars_main(self, unfiltered_items):
        # details for all cars ids
        car_ids = [x.get('id') for x in unfiltered_items]
        # details = self.get_separated_part_of_car_bulk('details', car_ids)  # for now we do not need details

        will_be_mailed_items = list()
        for li_car in unfiltered_items:
            # extract single item details from bulk details
            # single_car_details = [x for x in details if x['car_id'] == li_car['id']]  # no need for now
            if self.check_single_car(dict(li_car)):
                will_be_mailed_items.append(li_car)  # insert id of item that satisfy condition for further work
        if will_be_mailed_items:
            return will_be_mailed_items
