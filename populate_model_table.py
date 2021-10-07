from turbo_db_operations import TurboDbOperations


class PopulateModelTable(TurboDbOperations):
    def __init__(self):
        super(PopulateModelTable, self).__init__()

    def main(self):
        # pdb.set_trace()
        for i in range(1, 119):
            models = self.get_model_of_marka(i)
            for model in models:
                self.insert_into_model(i, model[0])

if __name__ == '__main__':
    PopulateModelTable().main()