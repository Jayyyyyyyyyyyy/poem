# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2021/01/28 4:48 PM
# @File      : candidates_routine.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import sys
import pandas as pd
path = sys.argv[1]
name1 = sys.argv[2]
name2 = sys.argv[3]
yesterday = sys.argv[4]

df = pd.read_csv(path, names = [name1, name2, 'count'])
df.to_csv('./relation_{}_{}-{}.csv'.format(name1, name2, yesterday), index=None)


# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql.types import *
# import time
# import os
# os.environ['PYSPARK_PYTHON']='/usr/local/bin/python'
# if __name__ == '__main__':
#     start = time.time()
#     reload(sys)
#     sys.setdefaultencoding('utf-8')
#     yesterday = sys.argv[1]
#     spark = SparkSession.builder \
#         .appName('knowledge_csv_file') \
#         .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
#         .enableHiveSupport().getOrCreate()
#     sql = 'SELECT content_mp3, cnt FROM (SELECT p_content_mp3["tagname"] AS content_mp3, count(*) AS cnt FROM dw.video_profile_parse WHERE dt = "{}" AND f_cstage IN (6,7,8,10) AND f_ctype IN (101,102,103,105,106,107,301,121) AND f_cstatus=0 GROUP BY p_content_mp3)a WHERE cnt > 10'.format(yesterday)
# sql = 'select * from  dw.video_profile_parse where dt="2021-01-27" limit 100'
# print(sql)
# df = spark.sql(sql).toPandas()
#     print('task done')
#     df.to_csv('./content_mp3-{}.csv'.format(yesterday), index=None)
#     spark.stop()


 # hive -e 'SELECT content_mp3, cnt FROM (SELECT p_content_mp3["tagname"] AS content_mp3, count(*) AS cnt FROM dw.video_profile_parse WHERE dt = "2021-02-03" AND f_cstage IN (6,7,8,10) AND f_ctype IN (101,102,103,105,106,107,301,121) AND f_cstatus=0 GROUP BY p_content_mp3)a WHERE cnt > 10' | sed 's/[\t]/,/g'  > ./content_mp3.csv
# hive -f content_mp3.sql | sed 's/[\t]/,/g'  > ./tmp_content_mp3.csv

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g csv_generator.py 2021-02-02 > running.log 2>&1


