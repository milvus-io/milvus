# -*- coding: utf-8 -*-

import logging
from email.mime.text import MIMEText
from email.header import Header
import smtplib

SMS_DEFAULT_TO_LIST = [
    # "zhifeng.zhang@zilliz.com",
    # "liang.liu@zilliz.com",
    # "zhenxiang.li@zilliz.com",
    "dev.milvus@zilliz.com"
]

def send_email(content="Test Failed", subject="Nightly Test Result", receivers=None):
    sender = 'test@milvus.io'
    message = MIMEText(content, 'html', 'utf-8')
    message['From'] = Header("test alert")
    message['To'] = Header("testAlerter")
    message['Subject'] = Header(subject, 'utf-8')
    try:
        smtp_obj = smtplib.SMTP('smtp.exmail.qq.com')
        if receivers is None:
            receivers = SMS_DEFAULT_TO_LIST
        smtp_obj.login(sender, "7UkFysXBGk2tb8Kz")
        result = smtp_obj.sendmail(sender, receivers, message.as_string())
    except smtplib.SMTPException as e:
        logging.error(str(e))
    finally:
        smtp_obj.quit()


if __name__ == "__main__":
    send_email()
