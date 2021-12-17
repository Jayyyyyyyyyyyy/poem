import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
    content_msg = MIMEText(contents,"plain","utf-8")
    msg.attach(content_msg)
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ";".join(to_list)
    with open(file,encoding='utf-8') as fp:
        attachment = MIMEText(fp.read(), _subtype=subtype)
    attachment.add_header("Content-Disposition", "attachment", filename=ntpath.basename(file))
    msg.attach(attachment)
    

    try:
        s = smtplib.SMTP()
        s.connect('smtp.exmail.qq.com')
        s.starttls()
        s.login("jiangcx@tangdou.com", "Ainiaiwo123")
        s.sendmail(me, to_list,msg.as_string())
        s.quit()
        return True
    except Exception as e:
        print ("error:" ,str(e))
        return False

def main():
    yesterday = datetime.now() - timedelta(days=1)
    yesterday = yesterday.strftime('%Y-%m-%d')
    me = "Jay<jiangcx@tangdou.com>"
    #to_list = ['jiangcx@tangdou.com','fangz@tangdou.com']
    to_list = ['maj@tangdou.com', 'yucj@tangdou.com', 'lijy@tangdou.com', 'jiangcx@tangdou.com']
    subject = 'ZERO RATE {} statistics'.format(yesterday)

    while True:
        file_path = '/data/jiangcx/user_dance_img_server/data/stats/stat_zero_rate_{}.csv'.format(yesterday)
        if os.path.exists(file_path):
            contents = '昨日用户0分率'
            send_mail(me, to_list, subject, contents, file_path)
            break
        else:
            time.sleep(300)

if __name__ == '__main__':
    main()

