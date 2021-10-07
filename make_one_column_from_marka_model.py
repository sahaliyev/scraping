import pdb

from db_queries import DBQueries


class OneColumn(DBQueries):
    def __init__(self):
        super(OneColumn, self).__init__()

    def main(self):
        old_values = self.get_old_marka_model()
        for value in old_values:
            car_id = value['id']
            marka = value['marka']
            model = value['model']

            marka_ = self.get_marka_id(marka)
            if not marka_:
                info = f"{car_id} car has marka problem"
                with open('marka_models_problems.txt', 'a', encoding="utf-8") as file:
                    print(info, file=file)
                continue
            model_ = self.get_model_id(marka_['id'], model)
            if not model_:
                info = f"{car_id} car has model problem"
                with open('marka_models_problems.txt', 'a', encoding="utf-8") as file:
                    print(info, file=file)
                continue

            self.update_car_mm(car_id, model_['id'])


if __name__ == '__main__':
    OneColumn().main()
