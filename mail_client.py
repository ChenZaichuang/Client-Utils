from email.mime.text import MIMEText
from email.header import Header
from smtplib import SMTP_SSL


class QQMailClient:

    def __init__(self, sender, pwd, receiver):

        self.sender = sender
        self.pwd = pwd
        self.receiver = receiver

    def send_mail(self, mail_title, mail_content):
        host_server = 'smtp.qq.com'
        sender_qq_mail = f'{self.sender}@qq.com'

        smtp = SMTP_SSL(host_server)
        smtp.set_debuglevel(0)
        smtp.ehlo(host_server)
        smtp.login(self.sender, self.pwd)

        msg = MIMEText(mail_content, "html", 'utf-8')
        msg["Subject"] = Header(mail_title, 'utf-8')
        msg["From"] = sender_qq_mail
        msg["To"] = self.receiver
        smtp.sendmail(sender_qq_mail, self.receiver, msg.as_string())
        smtp.quit()


def rows_to_table_string(rows):
    has_cell = False
    table_string = '<table style="border-collapse:collapse;">\n'
    for row in rows:
        table_string += '\t<tr>\n'
        for cell in row:
            has_cell = True
            table_string += f'\t\t<td style="border: 1px solid #000; padding: 10px; background-color: #eee; color: black">{cell}</td>\n'
        table_string += '\t</tr>\n'
    return table_string + '</table>\n' if has_cell else ''
