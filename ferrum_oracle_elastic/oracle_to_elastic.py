import pdb

from db_queries import DBQueries
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime


class OracleToElastic(DBQueries):
    def __init__(self):
        super(OracleToElastic, self).__init__()
        self.es = Elasticsearch([{'host': '172.16.10.222', 'port': 9200}])

    def get_greatest_id_for_index(self, index_, field_name):
        """
        @param index_: is index name in elasticsearch
        @param field_name: is field for id for this index in elasticsearch
        @return: greatest id for given index, if empty return 0
        """
        res = self.es.search(index=index_, body={
            'size': 1,
            "aggs": {
                "max_id": {"max": {"field": field_name}}
            }
        })

        try:
            # pdb.set_trace()
            latest_id = res['aggregations']['max_id']['value']
            if latest_id:
                return int(latest_id)
            else:
                return 0
        except (KeyError, ValueError):
            return 0

    def add_car_to_elastic(self, data, index_name, id_field):
        """
        @param data: come from oracle
        @param index_name: index that data will be inserted
        @param id_field: will be _id in this index
        """
        actions = [
            {
                "_index": index_name,
                "_id": dt.get(id_field),
                "_source": dt
            }
            for dt in data
        ]

        try:
            helpers.bulk(self.es, actions)
        except Exception as e:
            with open('logs/es.txt', 'a', encoding="utf-8") as file:
                print(f'ES error: {e}', file=file)

    def main(self):
        index_names = [
            {'index': 'reqress', 'id_field': 'ASM_SƏNƏD_ID'},
            {'index': 'hesablar', 'id_field': 'Kod'},
            {'index': 'provodkalar', 'id_field': 'Sənəd_ID'},
            {'index': 'customers', 'id_field': 'ID'},
            {'index': 'terminal', 'id_field': 'PAYMENT_ID'},
            {'index': 'finance_monthly', 'id_field': 'SENED_ID'},
            {'index': 'sazish', 'id_field': 'ASM_SENED_ID'},
        ]

        log = ""
        for item in index_names:
            # get greatest id from elastic for given index
            greatest_id = self.get_greatest_id_for_index(item['index'], item['id_field'])
            log += f"Greatest id for {item['index']} is {greatest_id}\n"
            # get data from oracle where id is greater than gretaest id
            if item['index'] == 'reqress':
                data = self.get_reqress(greatest_id)
                log += f"Data length {len(data)}\n"
            elif item['index'] == 'hesablar':
                data = self.get_hesablar(greatest_id)
                log += f"Data length {len(data)}\n"
            elif item['index'] == 'provodkalar':
                data = self.get_provodkalar(greatest_id)
                log += f"Data length {len(data)}\n"
            elif item['index'] == 'customers':
                data = self.get_customers(greatest_id)
                log += f"Data length {len(data)}\n"
            elif item['index'] == 'terminal':
                data = self.get_terminal(greatest_id)
                log += f"Data length {len(data)}\n"
            elif item['index'] == 'finance_monthly':
                data = self.finance_monthly_report(greatest_id)
                log += f"Data length {len(data)}\n"
            elif item['index'] == 'sazish':
                data = self.sazish(greatest_id)
                log += f"Data length {len(data)}\n"

            # import that data into elasticsearch
            self.add_car_to_elastic(data, item['index'], item['id_field'])
            log += f"Done with {item['index']}\n"
        log += f"at {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        log += 20 * "="
        print(log)
        with open('logs.txt', 'a', encoding="utf-8") as file:
            print(f"{log}\n", file=file)


if __name__ == '__main__':
    OracleToElastic().main()
