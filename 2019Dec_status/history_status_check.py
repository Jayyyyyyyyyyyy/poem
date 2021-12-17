# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/29 4:07 PM
# @File      : statv2_new.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang


import json
import sys
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
from collections import OrderedDict



def filter_no_cstatus(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    if 'cstatus' in row:
        return False
    else:
        return True


def filter_cstatus(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    if 'cstatus' in row:
        return True
    else:
        return False


def parse_id(row):
    row = row[1]
    id = row.split('\t')[0]
    return int(id)

def parse_row(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    status = row['cstatus']
    return (int(id), int(status))


def table_schema():
    col_Dict = OrderedDict()
    col_Dict['vid'] = StructField('vid', LongType(), True)
    col_Dict['cstatus'] = StructField('cstatus', IntegerType(), True)
    schema = StructType(list(col_Dict.values()))
    return schema

def filter_all_incomplete_profile(jsonobj):
    if 'id' not in jsonobj or 'ctype' not in jsonobj or 'cstatus' not in jsonobj or 'cstage' not in jsonobj or 'utime' not in jsonobj or 'etime' not in jsonobj or 'ctime' not in jsonobj or 'ptime' not in jsonobj or 'rtime' not in jsonobj or 'profileinfo' not in jsonobj or 'videoinfo' not in jsonobj:
        return True
    else:
        return False

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')


    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')

    sc = SparkContext(appName="test")
    yesterday_path = "hdfs://10.42.4.78:8020/user/jiaxj/Apps/DocsInfo/{}/*".format(yesterday)
    yesterday_data = sc.newAPIHadoopFile(yesterday_path, "com.hadoop.mapreduce.LzoTextInputFormat","org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")

    get_vid_without_cstatus = yesterday_data.filter(filter_no_cstatus).map(parse_id)

    data_rdd = yesterday_data.filter(filter_cstatus).map(parse_row)
    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()


    df2rdd = data_rdd.map(lambda x: Row(x[0], x[1]))
    # df_final = df2rdd.toDF(['vid', 'videourl', 'result'])
    df = spark.createDataFrame(df2rdd, table_schema())
    df.createOrReplaceTempView("hdfs_cstatus")


    not_same_status = "select db.vid, status, cstatus from (select vid, status from dw.video where status=0)db join  (select vid, cstatus from hdfs_cstatus)hdfs on(db.vid=hdfs.vid) where status <> cstatus"
    not_same_status_df = spark.sql(not_same_status)
    to_save1 = not_same_status_df.rdd.map(lambda x: "{}\t{}\t{}".format(x.vid,x.status,x.cstatus))

    lost_in_hdfs = "select db.vid, status, cstatus from (select vid, status from dw.video where status=0)db left join  (select vid, cstatus from hdfs_cstatus)hdfs on(db.vid=hdfs.vid) where hdfs.vid is null"
    lost_in_hdfs_df = spark.sql(lost_in_hdfs)
    to_save2 = lost_in_hdfs_df.rdd.map(lambda x: "{}\t{}\t{}".format(x.vid,x.status,x.cstatus))

    not_same_status_vids = not_same_status_df.select('vid').collect()
    not_same_status_vids = [int(row.vid) for row in not_same_status_vids]

    lost_in_hdfs_vids = lost_in_hdfs_df.select('vid').collect()
    lost_in_hdfs_vids = [int(row.vid) for row in lost_in_hdfs_vids]

    print(len(not_same_status_vids))
    print(len(lost_in_hdfs_vids))
    save_vids1 = "/user/jiangcx/his_diff_vids/status_not_same_{}/".format(yesterday)
    save_vids2 = "/user/jiangcx/his_diff_vids/hdfs_not_exist_{}/".format(yesterday)
    to_save1.saveAsTextFile(save_vids1)
    to_save2.saveAsTextFile(save_vids2)

    without_cstatus_in_hdfs_vids = get_vid_without_cstatus.collect()


    # vids = df.select('vid','status').collect()







    outpath = '/home/hadoop/users/jcx/hive/his_status_check/history_vids_status_{}.txt'.format(yesterday)
    with open(outpath, 'w') as write:
        write.write("\n\n{}   {}   {}\n".format("#" * 20, " hdfs and db compare", "#" * 20))

        write.write('task1: not exist in hdfs whereas in db: {}\n'.format(len(lost_in_hdfs_vids)))
        if len(lost_in_hdfs_vids)==0:
            write.write(' Well Done!\n')
        else:
            for item in lost_in_hdfs_vids:
                line = '{}\n'.format(item)
                write.write(line)

        write.write('task2: status of db and hdfs are not same: {}\n'.format(len(not_same_status_vids)))
        if len(not_same_status_vids)==0:
            write.write(' Well Done!\n')
        else:
            for item in not_same_status_vids:
                line = '{}\n'.format(item)
                write.write(line)

        write.write('task3: video profiles dont have cstatus field: {}\n'.format(len(without_cstatus_in_hdfs_vids)))
        if len(without_cstatus_in_hdfs_vids) == 0:
            write.write(' Well Done!\n')
        else:
            for item in without_cstatus_in_hdfs_vids:
                line = '{}\n'.format(item)
                write.write(line)


#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_status_check/history_status_check.py 2019-12-29 > running.log 2>&1
