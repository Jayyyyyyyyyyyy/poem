# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/1 2:08 PM
# @File      : spark_correct.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *



if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()

    # compare
    my_sql = "select u_key, max(pv) pv from da.query_ema_pv_middle where dt >='2020-02-12' and dt <='2020-02-18' group by u_key order by pv desc"
    df = spark.sql(my_sql)
    df = df.rdd.map(lambda x: json.dumps([x.u_key, x.pv],ensure_ascii=False))
    mypath = "/user/jiangcx/get_all_query_7days_0219"
    df.repartition(1).saveAsTextFile(mypath)


# spark-submit --master yarn --deploy-mode cluster  --num-executors 60 --executor-memory 6g  --executor-cores 4  --driver-memory 6g --archives hdfs://Ucluster/word_segment_jcx/segment_jcx.zip#segment   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./segment/segment_jcx/bin/python   /home/hadoop/users/jcx/hive/spark_word_segment/spark_correct.py 2019-09-28