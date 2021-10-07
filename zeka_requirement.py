import pandas as pd

from db_queries import DBQueries


class Zeka(DBQueries):
    def __init__(self):
        super().__init__()

    @staticmethod
    def one_model(marka_id, models):
        model_marka = list()
        for model in models:
            model = dict(model)
            model['marka_id'] = marka_id
            model_marka.append(model)

        return model_marka

    def all_models(self):
        all_model_markas = list()
        for marka_id in range(1, 178):
            models = self.get_model_of_marka(marka_id)
            all_model_markas.append(self.one_model(marka_id, models))
        return all_model_markas

    def main(self):
        df = pd.DataFrame()
        for list_ in self.all_models():
            new_df = pd.DataFrame(list_)
            df = pd.concat([df, new_df])
        df.reset_index()
        df.to_csv('excel/zeka.csv')


if __name__ == '__main__':
    Zeka().main()
