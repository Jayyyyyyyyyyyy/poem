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

def parse_row(row):
    row = row[1]
    row = row.split('\t')[1]
    row = json.loads(row, encoding='utf-8')
    if 'cstatus' in row:
        status = row['cstatus']
        if status == 0:
            if 'profileinfo' in row:
                if 'segment' in row['profileinfo'] and 'content_mp3_seg' in row['profileinfo']:
                    return False
                else:
                    return True
            else:
                return False
        else:
            return False
    else:
        return False
if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')

    sc = SparkContext(appName="test")
    yesterday_path = "hdfs://10.42.178.9:8020/user/jiaxj/Apps/DocsInfo/{}/*".format(yesterday)
    filerdd = sc.newAPIHadoopFile(yesterday_path,"com.hadoop.mapreduce.LzoTextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")

    data_rdd = filerdd.filter(parse_row)
    data_rdd = data_rdd.map(lambda x: x[1].split('\t')[1])
    ost_segments = "/user/jiangcx/lost_segments_field/{}/".format(yesterday_2)
    data_rdd.repartition(1).saveAsTextFile(ost_segments)
    # retry_data = sc.parallelize(list(diff)).saveAsTextFile(retry)

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g  lost_segment.py 2019-12-09