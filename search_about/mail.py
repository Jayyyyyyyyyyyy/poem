import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

import time
import psutil
import time
import os
from datetime import datetime, timedelta
import ntpath


def send_mail(me, to_list, subject, contents, file):
    # edit your email contents
    content_type = "application/octet-stream"
    _, subtype = content_type.split("/", 1)

    msg = MIMEMultipart()
    content_msg = MIMEText(contents, "plain", "utf-8")
    msg.attach(content_msg)
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ";".join(to_list)


    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(file, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', 'attachment', filename=ntpath.basename(file))
    msg.attach(part)

    #
    # with open(file, encoding='utf-8') as fp:
    #     attachment = MIMEText(fp.read(), _subtype=subtype)
    # attachment.add_header("Content-Disposition", "attachment", filename=ntpath.basename(file))
    # msg.attach(attachment)

    try:
        s = smtplib.SMTP()
        s.connect('smtp.exmail.qq.com')
        s.starttls()
        s.login("jiangcx@tangdou.com", "Ainiaiwo123")
        s.sendmail(me, to_list, msg.as_string())
        s.quit()
        return True
    except Exception as e:
        print("error:", str(e))
        return False


def main():
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    me = "Jay<jiangcx@tangdou.com>"
    to_list = ['jiangcx@tangdou.com']
    # to_list = ['jiangcx@tangdou.com', 'sunjian@tangdou.com', 'dux@tangdou.com', 'jianghr@tangdou.com']
    subject = '征集备选-{}'.format(yesterday)

    while True:
        file_path = '/data/jiangcx/candicates_{}.xlsx'.format(yesterday)
        if os.path.exists(file_path):
            contents = ''
            send_mail(me, to_list, subject, contents, file_path)
            break
        else:
            time.sleep(300)


if __name__ == '__main__':
    main()

