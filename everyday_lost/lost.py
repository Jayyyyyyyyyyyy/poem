# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/8/31 6:24 PM
# @File      : lost.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from datetime import timedelta

from pyspark.sql import Row
def func(lines):
    key = lines[0]
    value = lines[1]
    org = sorted(value, reverse=True)[0][1]
    return org

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    today_object = yesterday_object + timedelta(days=1)
    today = today_object.strftime('%Y-%m-%d')
    
    sc = SparkContext(appName="test")
    outpath = '/home/hadoop/users/jcx/hive/permanent_notification_bar/stat_videoprofile/diff_vids_{}.txt'.format(yesterday)
    
    yesterday_path = "/video_profile/{}/*/*".format(yesterday)
    today_path = "/video_profile/{}/*/*".format(today)
    yesterday_data = sc.textFile(yesterday_path)
    today_data = sc.textFile(today_path)
    
    # filter today records
    yesterday_total = yesterday_data.filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(
        lambda x: json.loads(x, encoding='utf-8')['videoinfo']['createtime'][:10] == yesterday)
    # group by latest records
    yesterday_remove_dup = yesterday_total.map(
        lambda x: (json.loads(x, encoding='utf-8')['id'], [json.loads(x, encoding='utf-8')['utime'], x]))
    yesterday_keep_latest_utime = yesterday_remove_dup.groupByKey().map(func)
    yesterday_vids = yesterday_keep_latest_utime.map(lambda x: json.loads(x, encoding='utf-8')['id']).collect()
    

    today_total = today_data.filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(
        lambda x: json.loads(x, encoding='utf-8')['videoinfo']['createtime'][:10] == yesterday)
    today_remove_dup = today_total.map(
        lambda x: (json.loads(x, encoding='utf-8')['id'], [json.loads(x, encoding='utf-8')['utime'], x]))
    today_keep_latest_utime = today_remove_dup.groupByKey().map(func)
    today_vids = today_keep_latest_utime.map(lambda x: json.loads(x, encoding='utf-8')['id']).collect()

    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()
    my_sql = "select vid from dw.video where to_date(createtime)='{}' ".format(yesterday)
    print(my_sql)
    df = spark.sql(my_sql)
    vids = df.select('vid').collect()
    yesterday_vids = yesterday_vids + today_vids
    yesterday_vids = set([int(x) for x in yesterday_vids])
    vids = set([int(row.vid) for row in vids])
    diff = vids.difference(yesterday_vids)
    print(diff)
    total_num = len(list(diff))
    with open(outpath, 'w') as write:
        write.write("\n\n{}   {}   {}\n".format("#" * 20, "vids can't find in video profile", "#" * 20))
        write.write('total number of missing: {}\n'.format(total_num))
        for vid in list(diff):
            line = '{}\n'.format(vid)
            write.write(line)

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g /home/hadoop/users/jcx/hive/permanent_notification_bar/lost.py 2019-09-03 > running.log 2>&1