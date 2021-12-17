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
#
# def gene_period(row):
#
#     id = json.loads(row, encoding='utf-8')['id']
#     title = json.loads(row, encoding='utf-8')['videoinfo']['title']
#     datenum = re.search('(201\d{4})', title, re.IGNORECASE).group(1)
#     dateobj = datetime.strptime(datenum, '%Y%m%d')
#
#


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    today_object = yesterday_object + timedelta(days=1)
    today = today_object.strftime('%Y-%m-%d')

    start = "2019-07-01"
    end = yesterday

    sc = SparkContext(appName="test")

    yesterday_path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format(yesterday)
    yesterday_data = sc.textFile(yesterday_path)

    # filter today records
    yesterday_total = yesterday_data.map(lambda x: x.split('\t')[1]).filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(re_match)
    # group by latest records
    yesterday_vids = yesterday_total.map(lambda x: json.loads(x, encoding='utf-8')['id']).collect()

    print(len(yesterday_vids))
    to_pickle = '/home/hadoop/users/jcx/hive/permanent_notification_bar/content_period_history_vids.pickle'
    pickle.dump(yesterday_vids, open(to_pickle, 'wb'), protocol=2)

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g /home/hadoop/users/jcx/hive/permanent_notification_bar/content_period_checker.py 2019-09-04 > running.log 2>&1