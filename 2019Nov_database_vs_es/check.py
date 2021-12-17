# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/18 5:40 PM
# @File      : check.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
import redis
import requests
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from datetime import timedelta

def func(lines):
    key = lines[0]
    value = lines[1]
    org = sorted(value, reverse=True)[0][1]
    return org

def to_redis(r,key, diff):
    for vid in list(diff):
        print(vid)
        r.lpush(key,vid)


def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    esvids = []
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}}, "_source": "content_mp3_seg"}

        payload = json.dumps(payload)
        print(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)

        no = []
        for x in response.json()['hits']['hits']:
            if 'content_mp3_seg' not in x['_source']:
                no.append(x['_id'])
        esvids.extend(no)
    return esvids
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    host = '10.19.127.152'
    port = 6379
    r = redis.Redis(host=host, port=port)
    key = 'rec:video:new:list'

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')
    today_object = yesterday_object + timedelta(days=1)
    today = today_object.strftime('%Y-%m-%d')

    sc = SparkContext(appName="test")
    yesterday_path = "/video_profile/{}/*/*".format(yesterday)
    yesterday_data = sc.textFile(yesterday_path)

    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()
    my_sql = "select vid from dw.video where to_date(createtime)<='{}' and status = 0".format(yesterday)
    my_sql = "select vid from dw.video where  status=0"
    df = spark.sql(my_sql)
    vids = df.select('vid').collect()
    vids = [int(row.vid) for row in vids]
    esvids = eschecker(vids)
    with open('./lost_seg_field', 'w') as write:
        for vid in esvids:
            line = '{}\n'.format(vid)
            write.write(line)


#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g /home/hadoop/users/jcx/hive/permanent_notification_bar/lost_and_redis.py 2019-09-03 > running.log 2>&1
