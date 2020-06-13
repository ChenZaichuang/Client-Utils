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

        msg = MIMEText(mail_content, "plain", 'utf-8')
        msg["Subject"] = Header(mail_title, 'utf-8')
        msg["From"] = sender_qq_mail
        msg["To"] = self.receiver
        smtp.sendmail(sender_qq_mail, self.receiver, msg.as_string())
        smtp.quit()
