# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/8/31 6:24 PM
# @File      : lost.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
import requests
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

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    today_object = yesterday_object + timedelta(days=1)
    today = today_object.strftime('%Y-%m-%d')

    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()
    my_sql = "select vid from dw.video where to_date(createtime)='{}' ".format(yesterday)
    df = spark.sql(my_sql)
    vids = df.select('vid').collect()
    vids = [int(row.vid) for row in vids]
    vid_list = [vids[i:i + 10000] for i in range(0, len(vids), 10000)]

    yeses = []

    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"query": {"terms": {"id": v_list}}, "_source": ""}
        payload = json.dumps(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("GET", url, data=payload, headers=headers)
        yes = [int(x['_id']) for x in response.json()['hits']['hits']]
        yeses.extend(yes)

    nos = list(set(vids).difference(set(yeses)))

    total_num = len(nos)
    outpath = '/home/hadoop/users/jcx/hive/permanent_notification_bar/stat_videoprofile/diff_vids_{}v2.txt'.format(
        yesterday)
    with open(outpath, 'w') as write:
        write.write("\n\n{}   {}   {}\n".format("#" * 20, "vids can't find in video profile", "#" * 20))
        write.write('total number of missing: {}\n'.format(total_num))
        for vid in nos:
            line = '{}\n'.format(vid)
            write.write(line)

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g /home/hadoop/users/jcx/hive/permanent_notification_bar/lostv2.py 2019-09-10 > running.log 2>&1




