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
    to_list = ['ai_all@tangdou.com','fangz@tangdou.com','lidg@tangdou.com']
    #to_list = ['jiangcx@tangdou.com','sunjian@tangdou.com','fangz@tangdou.com','jiaxj@tangdou.com','wangshuang@tangdou.com','pansm@tangdou.com','liujk@tangdou.com']
    subject = 'Lost vid in video profile {} statistics'.format(yesterday)
    file_path = '/home/hadoop/users/jcx/hive/permanent_notification_bar/stat_videoprofile/diff_vids_{}.txt'.format(yesterday)
    with open(file_path,'r',encoding='utf-8') as stat:
        res = stat.readlines()
        contents = ''.join(res)
    send_mail(me, to_list, subject, contents, file_path)

if __name__ == '__main__':
    main()