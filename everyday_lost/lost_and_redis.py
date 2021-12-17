# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/8/31 6:24 PM
# @File      : lost.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
import redis
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from datetime import timedelta

def func(lines):
    key = lines[0]
    value = lines[1]
    org = sorted(value, reverse=True)[0][1]
    return org

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    host = '10.19.127.152'
    port = 6379

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')
    today_object = yesterday_object + timedelta(days=1)
    today = today_object.strftime('%Y-%m-%d')

    sc = SparkContext(appName="test")

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
    r = redis.Redis(host=host, port=port)
    key = 'rec:video:new:list'
    # for vid in list(diff):
    #     print(vid)
    #     r.lpush(key,vid)
    time = datetime.today().strftime('%Y-%m-%d %H-%M-%S')
    #retry = "/redis_retry/{}/".format(yesterday_2)
    #retry_data = sc.parallelize(list(diff)).saveAsTextFile(retry)
    print('{} : Done!'.format(time))

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g /home/hadoop/users/jcx/hive/permanent_notification_bar/lost_and_redis.py 2019-09-03 > running.log 2>&1