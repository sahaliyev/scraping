from turbo_db_operations import TurboDbOperations
import pandas as pd


class FullRequirements(TurboDbOperations):
    def __init__(self):
        super(FullRequirements, self).__init__()

    def my_reqs(self):
        new_list = list()
        reqs = self.get_all_requirements()
        for req in reqs:
            req = dict(req)
            req_id = req.get('id')
            bans = self.get_part_of_requirement('requirements_ban_type', req_id)
            ban_ids = [x.get('ban_type_id') for x in bans]
            if ban_ids:
                bb = self.get_joined_part_of_requirements('ban_type', ban_ids)
                req['ban_type'] = ', '.join([x[0] for x in bb])

            details = self.get_part_of_requirement('requirements_details', req_id)
            detail_ids = [x.get('details_id') for x in details]
            if detail_ids:
                dd = self.get_joined_part_of_requirements('unique_details', detail_ids)
                req['details'] = ', '.join([x[0] for x in dd])

            fuel = self.get_part_of_requirement('requirements_fuel_type', req_id)
            fuel_ids = [x.get('fuel_type_id') for x in fuel]
            if fuel_ids:
                ff = self.get_joined_part_of_requirements('fuel_type', fuel_ids)
                req['fuel_type'] = ', '.join([x[0] for x in ff])

            gear = self.get_part_of_requirement('requirements_gearbox', req_id)
            gear_ids = [x.get('gearbox_id') for x in gear]
            if gear_ids:
                gg = self.get_joined_part_of_requirements('gearbox', gear_ids)
                req['gearbox'] = ', '.join([x[0] for x in gg])
            new_list.append(req)
        return new_list

    def main(self):
        result = self.my_reqs()
        df = pd.DataFrame(result)
        df.to_excel('excel/requirements_new.xlsx', index=False)


if __name__ == '__main__':
    fr = FullRequirements()
    fr.main()
