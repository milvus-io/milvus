# -*- coding: utf-8 -*-
import sys
import logging
from email.mime.text import MIMEText
from email.header import Header
import smtplib

SMS_DEFAULT_TO_LIST = [
    "dev.milvus@zilliz.com",
]

def send_email(subject, content, token, receivers=None):
    sender = 'test@zilliz.com'
    message = MIMEText(content, 'html', 'utf-8')
    message['From'] = Header("Daily Test")
    message['To'] = Header("dev.milvus")
    message['Subject'] = Header(subject, 'utf-8')
    try:
        smtp_obj = smtplib.SMTP('smtp.exmail.qq.com')
        if receivers is None:
            receivers = SMS_DEFAULT_TO_LIST
        smtp_obj.login(sender, token)
        result = smtp_obj.sendmail(sender, receivers, message.as_string())
    except smtplib.SMTPException as e:
        logging.error(str(e))
    finally:
        smtp_obj.quit()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit()
    subject = sys.argv[1]
    content = sys.argv[2]
    token = sys.argv[3]
    send_email(subject, content, token)
