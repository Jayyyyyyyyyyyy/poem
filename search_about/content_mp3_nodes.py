# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/20 4:48 PM
# @File      : get_fields.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
import time


if __name__ == '__main__':
    start = time.time()
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')

    sc = SparkContext(appName="test")
    yesterday_path = "hdfs://10.42.178.9:8020/user/jiaxj/Apps/DocsInfo/{}/*".format(yesterday)
    yesterday_data = sc.newAPIHadoopFile(yesterday_path, "com.hadoop.mapreduce.LzoTextInputFormat",
                                         "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")


    # load mp3 dict start
    import json
    dict = {}
    mp3_count = '/Users/jiangcx/Downloads/mp3.all.json'
    with open(mp3_count, 'r', encoding='utf-8') as f:
        for line in f:
            line = json.loads(line)
            if line['type'] in (6, 8) and line['flag'] in (0, 1):
                if line['qcmp3'] == line['mp3']:
                    dict[line['qcmp3']] = [int(line['video_count']), line['rescount1']]
    ### end

    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()


    sql = "select p_content_mp3['tagname'] as content_mp3, count(*) as cnt from dw.video_profile_parse where dt = date_sub(current_date ,1) and f_cstage in (6,7,8,10) and f_ctype in (101, 102, 103, 105, 106, 107, 301, 121) and f_cstatus=0 group by p_content_mp3"
    sqldf = spark.sql(sql)
    my_df = sqldf.rdd.map(lambda x: [x.content_mp3, x.cnt]).collect()
    out_path = './content_mp3_nodes'
    with open(out_path,'w') as w:
        for content_mp3, cnt in my_df:
            if content_mp3 in dict:
                line = json.dumps({content_mp3:cnt})
            w.write(line+'\n')



#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_content_mp3_update_all/get_field_es.py 2020-01-08 > running.log 2>&1


