# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2021/01/28 4:48 PM
# @File      : candidates_routine.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
import time
import pandas as pd
import os
os.environ['PYSPARK_PYTHON']='/usr/local/bin/python'



if __name__ == '__main__':
    start = time.time()
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')

    sc = SparkContext(appName="Candidates for generate")
    mp3_count = sc.textFile('/tmp/sunjian/allmp3/{}/mp3.all.json{}'.format(yesterday,yesterday)).collect()
    dict = {}
    for line in mp3_count:
        line = json.loads(line)
        if 'type' not in line:
            continue
        if line['type'] in (6,8) and line['flag'] in (0,1):
            if line['qcmp3'] == line['mp3']:
                dict[line['qcmp3']] = [int(line['video_count']), line['rescount1']]
    print('task1')
    # incomplete_ids = yesterday_data.filter(filter_cstatus).filter(filter_parse_row2).map(parse_id)
    # ids = "/user/jiangcx/his_diff_vids/ids_{}/".format(yesterday)
    # incomplete_ids.saveAsTextFile(ids)
    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()

    sql = 'select d_key, search_uv, click_pv, usefull_pv, exp_pv  from  app.app_search_allmodule_d where dt=date_sub(current_date ,1) and exp_pv>5 and usefull_pv>5'
    df = spark.sql(sql).toPandas()


    df['video_count'] = df.apply(lambda x: dict[x['d_key']][0] if x['d_key'] in dict else -1, axis=1)
    df['rescount1'] = df.apply(lambda x: dict[x['d_key']][1] if x['d_key'] in dict else -1, axis=1)
    df = df.loc[lambda x: x['video_count'] > 1]
    df = df.loc[lambda x: x['usefull_pv'] > 5]

    df.eval("""
    eval = (click_pv/usefull_pv)*(1/log(video_count))
    """, inplace=True)
    tmpdf = df.loc[lambda x: x['rescount1'] < 20]
    mydf = tmpdf[['d_key', 'search_uv', 'click_pv', 'usefull_pv', 'exp_pv', 'video_count', 'rescount1', 'eval']].sort_values('search_uv',
                                                                                                              ascending=False)
    mydf = mydf.rename(columns={'d_key':'搜索词', 'search_uv':'搜索人数', 'click_pv':'点击次数', 'usefull_pv':'好用次数', 'exp_pv':'曝光次数'})
    mydf.to_csv('./candicates_{}.csv'.format(yesterday), index=None)

    spark.stop()

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g routine.py 2021-01-27 > running.log 2>&1


