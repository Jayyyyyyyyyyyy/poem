# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/21 5:45 PM
# @File      : compare_titler.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import sys
from pyspark.sql import SparkSession
from pyspark.sql import Row


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()

    es_vid_title_path = "/tmp/sunjian/lostvids/vid2title/*"
    es_vid_title_rdd = spark.sparkContext.textFile(es_vid_title_path)
    es_vid_title_rdd = es_vid_title_rdd.map(lambda x: x.split('\t'))
    es_vid_title_rdd = es_vid_title_rdd.filter(lambda x: len(x) == 2)
    es_vid_title_df = es_vid_title_rdd.map(lambda x: Row(x[0], x[1])).toDF(['vid','title'])
    es_vid_title_df.registerTempTable('tmp_table')
    diff = " select vid, status, db_title, hdfs_title from  (select a.vid, status, a.title db_title, b.title hdfs_title from (select vid, status, title from dw.video)a join (select vid, title from tmp_table)b on(a.vid=b.vid))aa where db_title <> hdfs_title"
    diff_df = spark.sql(diff)

    diff_df_rdd = diff_df.rdd.map(lambda x: str(x.vid)+'\t'+str(x.status)+'\t'+str(x.db_title.encode('utf-8'))+'\t'+str(x.hdfs_title.encode('utf-8')))
    print(diff_df_rdd.take(100))
    diff_df_rdd = diff_df_rdd.saveAsTextFile("hdfs://Ucluster/word_segment_result/diff_vid_title_res")

# spark-submit --master yarn --deploy-mode cluster  --num-executors 60 --executor-memory 24g  --executor-cores 4  --driver-memory 24g  compare_title.py
