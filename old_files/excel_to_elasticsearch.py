import pdb

from elasticsearch import Elasticsearch
import pandas as pd
from elasticsearch.helpers import bulk


class ExcelToElasticsearch:
    def __init__(self):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def doc_generator(self, df):
        df_iter = df.iterrows()
        for index, document in df_iter:
            document = document.dropna()
            document = self.make_dict(document)
            # self.es.index(index='ferrum', id=document['Kod'], body=document)
            yield {
                "_index": 'ferrum',
                "_id": f"{document['Kod']}",
                "_source": document,
            }

    @staticmethod
    def make_dict(single_row):
        # pdb.set_trace()
        my_dict = dict()
        columns = ['Kod', 'Sənəd nömrəsi', 'Əlavə SN', 'Qurum', 'Partnyor', 'PGZ',
                   'Mağaza', 'Məhsul', 'Sifarişin mənbəyi', 'Ekspert', 'Müştəri',
                   'Zaminlikləri', 'FinKod', 'İlkin ödəniş', 'Məbləğ', 'Müddət',
                   'Gecikmə %', 'AS kom1', 'Tarix', 'Sənəd mərhələsi', 'İmtina səbəbi',
                   'RQO', 'Reqres', 'SB', 'Saziş', 'PK', 'MT', 'PM', 'Müraciət', 'PKE',
                   'MÇS', 'MÇS müəllifi', 'Sənəd statusu', 'Partnyor üzrə satıcı',
                   'Partnyor üzrə TM', 'SQ', 'SQ müəllifi', 'SQ tarixi']
        for item in columns:
            try:
                my_dict[item] = single_row[item]
            except KeyError:
                continue
        return my_dict

    def main(self):
        # pdb.set_trace()
        df = pd.read_excel('excel/bom.xlsx')
        bulk(self.es, self.doc_generator(df))


if __name__ == '__main__':
    ExcelToElasticsearch().main()
