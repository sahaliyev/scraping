from datetime import datetime

from filter_cars import FilterCars
from send_mail import SendMail


class FilterAndSendMail(FilterCars, SendMail):
    def __init__(self):
        super(FilterAndSendMail, self).__init__()

    @staticmethod
    def sort_by_date(e):
        return e['date']

    def filter_and_send_mail_main(self):
        receiver_emails = self.get_receiver_emails()
        # unfiltered_items = self.get_unfiltered_items_test()
        unfiltered_items = self.get_unfiltered_items()
        joined_items = list()
        full_featured_cars = list()

        if unfiltered_items:
            current_time = datetime.strftime(datetime.now(), "%d.%m.%Y %H:%M")
            filtered_items = self.filter_cars_main(unfiltered_items)
            if filtered_items:
                filtered_item_ids = [x.get('id') for x in filtered_items]
                joined_items = self.join_car_to_parts_query(filtered_item_ids)
                for item in joined_items:
                    item = dict(item)
                    item['avg_price'] = self.get_average_price_based_on_marka_and_model_and_year(item['mm'],
                                                                                                 item['year'])[0]
                    new_price = self.get_price_in_updated_car_price_table(item['id'])
                    if new_price:
                        item['new_price'] = new_price['price']
                    else:
                        item['new_price'] = 'New'
                    full_featured_cars.append(item)

                # full_featured_cars.sort(key=self.sort_by_date)
                self.send_mail_main(full_featured_cars, receiver_emails)
            unfiltered_items_ids = [x.get('id') for x in unfiltered_items]
            # this is temp removal !!!!!!!!!
            self.mark_item_as_filtered(unfiltered_items_ids)
            print(f"{len(joined_items)} items out of "
                  f"{len(unfiltered_items)} at {current_time}")  # for cron log


if __name__ == '__main__':
    f = FilterAndSendMail()
    f.filter_and_send_mail_main()
