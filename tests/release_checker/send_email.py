import smtplib
import constants as cons
from smtplib import SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from utils.util_log import test_log as log

def send_email(to_addr, cc_addr, message, table=None):

    if not isinstance(to_addr, list) or not isinstance(cc_addr, list):
        raise Exception("recieve and cc address should be list type")

    from_addr = cons.RELEASE_EMAIL_USERNAME
    password = cons.RELEASE_EMAIL_PASSWORD
    smtp_server = cons.RELEASE_SMTP_SERVER

    # add email body
    msg = MIMEMultipart()
    msg.attach(MIMEText("<br>" + message + "<br>" + "<br>", "html", "utf-8"))
    # add table
    if table != None:
        msg.attach(MIMEText(table, "html", "utf-8"))
        msg = add_attach(msg, "check_item_table.txt", "check_item_table.txt")
    common_message = "Any questions, please contact QA team"
    msg.attach(MIMEText("<br>" + common_message, "html", "utf-8"))

    # add attachment
    msg = add_attach(msg, "ci_test_log.log", "release.log")

    msg["From"] = Header(from_addr)
    msg["To"] = Header(",".join(to_addr))
    if len(cc_addr) > 0:
        to_addr.extend(cc_addr)
        msg["CC"] = Header(",".join(cc_addr))
    msg["Subject"] = Header("Milvus %s release results" % cons.milvus_release_version)

    try:
        server = smtplib.SMTP_SSL(host=smtp_server)
        server.connect(smtp_server, 465)

        server.login(from_addr, password)
        server.sendmail(from_addr, to_addr, msg.as_string())
        server.quit()
    except smtplib.SMTPException as e:
        log.error("Email sent fail: %s" % str(e))

    return True

def add_attach(msg, old_file_name, rename):

    file_name = "%s/%s" % (cons.release_log_path, old_file_name)
    att = MIMEText(open(file_name, "rb").read(), "base64", "utf-8")
    attch_name = "v" + cons.milvus_release_version + "-%s" % rename
    att["Content-Type"] = "application/octet-stream"
    att['Content-Disposition'] = "attachment;filename='%s'" % attch_name
    msg.attach(att)

    return msg
