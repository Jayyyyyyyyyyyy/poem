# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/4/7 10:33 AM
# @File      : query_tag.py
# @Software  : PyCharm

import json
import sys
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
from collections import OrderedDict
import re


# import json
# def parse_row_video(row):
#     row = row[1]
#     id = row.split('\t')[0]
#     row = json.loads(row.split('\t')[1], encoding='utf-8')
#     if 'keywords' in row['profileinfo'] and type(row['profileinfo']['keywords']) == type('abc') :
#         return True
#     else:
#         return False


def filter_row_video(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    try:
        status = row['cstatus']
        if status != 0:
            return False
        video_reprint_flag = row['profileinfo']['video_reprint_flag']
        if video_reprint_flag == 1:
            return True
        else:
            return False
    except:
        return False


def parse_row_video(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    if 'profileinfo' in row and 'video_reprint_flag' in row['profileinfo']:
        video_reprint_flag = row['profileinfo']['video_reprint_flag']
    else:
        video_reprint_flag = 0

    return (int(id), video_reprint_flag)


def table_schema_video():
    col_Dict = OrderedDict()
    col_Dict['vid'] = StructField('vid', LongType(), True)
    col_Dict['video_quality2'] = StructField('video_quality2', StringType(), True)
    col_Dict['video_quality3'] = StructField('video_quality3', StringType(), True)
    col_Dict['pic'] = StructField('pic', StringType(), True)
    schema = StructType(list(col_Dict.values()))
    return schema


from collections import Counter



def to_json_teacher_video(vid, video_reprint_flag):
    tmp = {}
    tmp['vid'] = vid
    tmp['video_reprint_flag'] = video_reprint_flag
    final_res = json.dumps(tmp, ensure_ascii=False)
    return final_res


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')
    sc = SparkContext(appName="query_tag")

    spark = SparkSession.builder \
        .appName('query_tag') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()
    partitions = 40
    spark.sql("SET spark.sql.shuffle.partitions={}".format(partitions))
    spark.sql("SET mapreduce.reduce.memory.mb = 5120;")
    spark.sql("SET mapreduce.map.memory.mb = 5120;")
    #### from video profile
    ### comment start
    yesterday_videoprofile_path = "hdfs://10.42.178.9:8020/user/jiaxj/Apps/DocsInfo/{}/*".format(yesterday)
    parse_data = sc.newAPIHadoopFile(yesterday_videoprofile_path, "com.hadoop.mapreduce.LzoTextInputFormat",
                                     "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
    # 画像数据太大，先过滤出带content_mp3的数据
    parse_data = parse_data.filter(filter_row_video).map(parse_row_video)
    # print parse_data.count()
    df2rdd_video = parse_data.map(lambda x: to_json_teacher_video(x[0], x[1]))
    df2rdd_video.saveAsTextFile("/user/jiangcx/video_reprint/{}/".format(yesterday))
    ### comment end
    spark.stop()
# /home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn  --num-executors 4 --executor-cores 2 --executor-memory 4g --driver-memory 4g  --conf spark.driver.maxResultSize=4g query_tag_2.py 2020-07-06
