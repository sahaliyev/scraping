import pdb
import sys

sys.path.append('../')
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from db_queries import DBQueries
import pandas as pd


class SendMail(DBQueries):
    def __init__(self):
        super(SendMail, self).__init__()
        self.current_date = datetime.now().strftime("%d.%m.%Y")

    SENDER_EMAIL = 'sahil.aliyev.751@gmail.com'
    SENDER_PASSWORD = 'Sahil2018'
    SERVER = 'smtp.gmail.com:465'
    RECEIVER_EMAIL = ['sahil.eyev@gmail.com', 'ali.khayyam@ferrumcapital.az']
    # RECEIVER_EMAIL = ['sahil.eyev@gmail.com']  # test only me
    SUBJECT = 'New Cars'

    HTML = """
    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8" />
        <style type="text/css">
          table {
            background: white;
            border-radius:3px;
            border-collapse: collapse;
            height: auto;
            padding:5px;
            width: 100%;
            animation: float 5s infinite;
          }
          th {
            color:#fff;;
            background:#BA3A2C;
            font-size:14px;
            padding:10px;
            text-align:center;
            vertical-align:middle;
          }
          tr {
            border-top: 1px solid #C1C3D1;
            border-bottom: 1px solid #C1C3D1;
            border-left: 1px solid #C1C3D1;
            color:#666B85;
            font-size:16px;
            font-weight:normal;
          }
          td {
            background:#FFFFFF;
            padding:10px;
            text-align:left;
            vertical-align:middle;
            font-weight:300;
            font-size:13px;
            border-right: 1px solid #C1C3D1;
          }
        </style>
      </head>
      <body>
        <div style="overflow-x:auto;">
        <table>
          <thead>
            <tr style="border: 1px solid #1b1e24;">
    """

    def _generate_message(self) -> MIMEMultipart:
        message = MIMEMultipart("alternative", None, [MIMEText(self.HTML, 'html')])
        message['Subject'] = f"{self.SUBJECT} {self.current_date}"
        message['From'] = "Cars Automation <sahil.aliyev.751@gmail.com>"
        message['To'] = ', '.join(self.RECEIVER_EMAIL)
        return message

    def send_message(self):
        message = self._generate_message()
        server = smtplib.SMTP_SSL(self.SERVER)
        server.ehlo()
        # server.starttls()
        server.login(self.SENDER_EMAIL, self.SENDER_PASSWORD)
        server.sendmail(self.SENDER_EMAIL, self.RECEIVER_EMAIL, message.as_string())
        server.quit()

    def send_mail_main(self):
        preferred_columns = ['marka', 'model', 'year', 'price', 'new_price', 'seller', 'phone', 'url']
        for item in preferred_columns:
            self.HTML += f'<th>{item.capitalize()}</th>'
        self.HTML += "</tr></thead><tbody>"

        cars = self.send_mail_marketing_query()
        # pdb.set_trace()
        # new_cars = list()
        for car in cars:
            # with open('logs.txt', 'a', encoding="utf-8") as file:
            #     print(f"{car['id']} is parsed!", file=file)
            car = dict(car)
            numbers = ', '.join([x['unique_phone_number'] for x in self.get_car_phone_numbers(car['id'])])
            new_price = self.get_price_in_updated_car_price_table(car['id'])
            if car['price'] < 9500:
                continue
            # pdb.set_trace()
            if new_price:
                if new_price['price'] < 9500:
                    continue
                car['new_price'] = new_price['price']
            else:
                car['new_price'] = 'New'

            if numbers:
                car['phone'] = numbers
            # new_cars.append(car)
            my_res = f""
            for item in preferred_columns:
                my_res += f"<td>{car[item]}</td>"
            self.HTML += f"<tr>{my_res}</tr>"
        self.HTML += ' </tbody></table></div></body></html>'
        self.send_message()
        print(f"Send: {len(cars)} items at {self.current_date}")  # for cron log
        # df = pd.DataFrame(new_cars)
        # df.to_excel('all_turbo_data.xlsx', index=False, columns=preferred_columns)


if __name__ == '__main__':
    SendMail().send_mail_main()
