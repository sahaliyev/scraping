import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class SendMail:
    def __init__(self):
        super(SendMail, self).__init__()

    current_date = datetime.now().strftime("%d.%m.%Y")

    SENDER_EMAIL = 'sahil.aliyev.751@gmail.com'
    SENDER_PASSWORD = 'Sahil2018'
    SERVER = 'smtp.gmail.com:465'
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

    def _generate_message(self, receiver_emails) -> MIMEMultipart:
        message = MIMEMultipart("alternative", None, [MIMEText(self.HTML, 'html')])
        message['Subject'] = f"{self.SUBJECT} {self.current_date}"
        message['From'] = "Cars Automation <sahil.aliyev.751@gmail.com>"
        message['To'] = ', '.join(receiver_emails)
        return message

    def send_message(self, receiver_emails):
        receiver_emails = [x[0] for x in receiver_emails]
        # receiver_emails = ['sahil.eyev@gmail.com']

        message = self._generate_message(receiver_emails)
        server = smtplib.SMTP_SSL(self.SERVER)
        server.ehlo()
        # server.starttls()
        server.login(self.SENDER_EMAIL, self.SENDER_PASSWORD)
        server.sendmail(self.SENDER_EMAIL, receiver_emails, message.as_string())
        server.quit()

    def send_mail_main(self, list_of_cars, receiver_emails):
        preferred_columns = ['id', 'marka', 'model', 'year', 'used_by_km', 'price', 'new_price',
                             'avg_price', 'fuel_type', 'engine', 'url']
        for item in preferred_columns:
            self.HTML += f'<th>{item.capitalize()}</th>'
        self.HTML += "</tr></thead><tbody>"

        row_counter = 1
        for car in list_of_cars:
            my_res = f""
            for item in preferred_columns:
                # if item == 'date':
                #     my_res += f"<td>{datetime.strftime(car[item], '%d.%m.%Y %H:%M')}</td>"
                my_res += f"<td>{car[item]}</td>"
            row_counter += 1
            self.HTML += f"<tr>{my_res}</tr>"
        self.HTML += ' </tbody></table></div></body></html>'
        self.send_message(receiver_emails)
