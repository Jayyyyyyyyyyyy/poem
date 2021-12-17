# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/04 6:24 PM
# @File      : lost.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
import re
import pickle
import time
from pyspark import SparkContext
from datetime import datetime
from datetime import timedelta


def func(lines):
    key = lines[0]
    value = lines[1]
    org = sorted(value, reverse=True)[0][1]
    return org


def re_match(row):
    title = json.loads(row, encoding='utf-8')['videoinfo']['title']
    date_search = re.search('201(\d{4})', title, re.IGNORECASE)
    if date_search:
        return True
    else:
        return False


def filter_utime(row):
    createtime = json.loads(row, encoding='utf-8')['videoinfo']['createtime'][:19]
    if 'etime' not in json.loads(row, encoding='utf-8'):
        return True
    else:
        etime = json.loads(row, encoding='utf-8')['etime']
    dateobj = datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S')
    t = dateobj.timetuple()
    timeStamp = int(time.mktime(t))
    intervel = etime - timeStamp * 1000
    if intervel > 31622400000:  # 大于366天
        return True
    else:
        return False


def gene_period(row):
    tmp = {}
    vid = json.loads(row, encoding='utf-8')['id']
    createtime = json.loads(row, encoding='utf-8')['videoinfo']['createtime'][:19]
    dateobj = datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S')
    t = dateobj.timetuple()
    timeStamp = int(time.mktime(t)) * 1000
    etime = timeStamp + 31622400000
    tmp['id'] = vid
    tmp['etime'] = etime
    return tmp

    # id = json.loads(row, encoding='utf-8')['id']
    # title = json.loads(row, encoding='utf-8')['videoinfo']['title']
    # datenum = re.search('(201\d{4})', title, re.IGNORECASE).group(1)
    # dateobj = datetime.strptime(datenum, '%Y%m%d')


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    today_object = yesterday_object + timedelta(days=1)
    today = today_object.strftime('%Y-%m-%d')

    sc = SparkContext(appName="test")

    yesterday_path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format(yesterday)
    yesterday_data = sc.textFile(yesterday_path)

    # filter today records
    yesterday_total = yesterday_data.map(lambda x: x.split('\t')[1]).filter(lambda x: 'profileinfo' in json.loads(x, encoding='utf-8'))
    has_mp3_tagname = yesterday_total.filter(lambda x : 'content_mp3' in json.loads(x, encoding='utf-8')['profileinfo']).filter(lambda x : 'tagname' in json.loads(x, encoding='utf-8')['profileinfo']['content_mp3'])
    stat_mp3_gene = has_mp3_tagname.map(lambda x: (json.loads(x, encoding='utf-8')['profileinfo']['content_mp3']['tagname'],1))
    res_mp3 = stat_mp3_gene.reduceByKey(lambda x, y: x+y).sortBy(lambda a: a[1]).collect()
    outpath = '/home/hadoop/users/jcx/hive/permanent_notification_bar/stat_videoprofile/result_content_mp3_names.txt'

    with open(outpath, 'w') as write:
        for x, y in reversed(res_mp3):
            if j >= 100:
                break
            line = '{}\n'.format(x)
            write.write(line)

    # /home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g