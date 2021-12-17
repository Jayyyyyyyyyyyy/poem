#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : '2019/6/19 下午2:44'
@Author  : 'caoyongchuang(caoyc@tangdou.com)'
@File    : 'build_item_ft.py'
"""
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
import json
from datetime import datetime, timedelta
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql import Row, SparkSession
ITEM_RAW_DIR = "hdfs://10.42.31.63:8020/video_profile/"
REC_RAW_DIR = "/video_profile_jcx_tmp/"
ITEM_RAW_CLEAN_DIR = "/olap/da/video_clean_jcx_tmp/"
ITEM_FEATURE = "/olap/da/recy_it_ft_tmp_jcx/"

import hadoop
from optparse import OptionParser



def main():
    mode_date = (datetime.today() - timedelta(1)).strftime("%Y-%m-%d")
    his_date = (datetime.today() - timedelta(2)).strftime("%Y-%m-%d")
    del_date = (datetime.today() - timedelta(4)).strftime("%Y-%m-%d")
    spark = SparkSession.builder.master('yarn-client') \
        .appName('build_it_ft:' + mode_date) \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()
    args = map(lambda x:x.lstrip("r") if x.startswith("r--") else x, sys.argv[1:])
    parser = OptionParser()
    parser.add_option("--start_day", dest="start_day", default=mode_date, help="item history start day")
    parser.add_option("--end_day", dest="end_day", default=mode_date, help="item history end day")
    parser.add_option("--save_date", dest="save_date", default=mode_date, help="feature index save date")
    parser.add_option("--his_date", dest="his_date", default=his_date, help="feature index save date")
    (flags, args) = parser.parse_args(args)
    sc = spark.sparkContext
    item_raw = load_item(sc, flags.start_day, flags.end_day)
    if item_raw is None:
        copy_history(sc, ITEM_RAW_CLEAN_DIR, flags.save_date, flags.his_date)
    else:
        item_new = item_raw.groupByKey()\
            .map(sorted_item)
        item_new_save = item_new.map(save2json)
        merge_history(sc, item_new_save, ITEM_RAW_CLEAN_DIR, flags.save_date, flags.his_date)
    #hadoop.rm_dir(REC_RAW_DIR + del_date)
    #hadoop.rm_dir(ITEM_RAW_CLEAN_DIR+ del_date)




def copy_history(sc, ITEM_FEATURE, mode_date, his_date):
    save_dir = ITEM_FEATURE + mode_date
    #hadoop.rm_dir(save_dir)
    if hadoop.is_dir_exist(ITEM_FEATURE + his_date) == 0:
        hadoop.distcp(ITEM_FEATURE + his_date, save_dir)


def merge_history(sc, item_new, ITEM_FEATURE, mode_date, his_date):
    save_dir = ITEM_FEATURE + mode_date
    #hadoop.rm_dir(save_dir)
    if hadoop.is_dir_exist(ITEM_FEATURE + his_date) != 0:
        item_all = item_new.map(lambda x: "\t".join([str(x[0]), x[1]]))
    else:
        item_history = sc.textFile(ITEM_FEATURE + his_date) \
            .map(lambda x: x.strip().split("\t"))
        item_all = item_history.fullOuterJoin(item_new) \
            .map(parse_item)
    item_all.saveAsTextFile(save_dir)


def load_item(sc, start_day, end_day):
    start_day = datetime.strptime(start_day, "%Y-%m-%d")
    end_day = datetime.strptime(end_day, "%Y-%m-%d")
    item_total = []
    while start_day <= end_day:
        day = start_day.strftime("%Y-%m-%d")
        in_dir = ITEM_RAW_DIR + day
        out_dir = REC_RAW_DIR + day
        #hadoop.rm_dir(out_dir)
        hadoop.distcp(in_dir, out_dir)
        for hour in range(24):
            hour_dir = "/" + "{:02d}".format(hour)
            dir = REC_RAW_DIR + day + hour_dir
            if hadoop.is_dir_exist(dir) ==0:
                item_raw = sc.textFile(dir)\
                    .map(parse_line)\
                    .filter(lambda x: x is not None)
                if item_raw is not None:
                    item_total.append(item_raw)
        start_day = start_day + timedelta(1)
    if len(item_total) ==0:
        return None
    return sc.union(item_total)

def parse_line(line):
    try:
        line =json.loads(line)
    except Exception as e:
        return None
    else:
        return [line['id'], (line['utime'], line)]

def save2json(line):
    vid, item_ft = line
    try:
        item_ft = json.dumps(item_ft, ensure_ascii=False)
    except Exception:
        return None
    return [vid, item_ft]


def parse_item(line):
    vid,(history, new) = line
    if history is None:
        out= [vid, new]
    elif new is None:
        out= [vid, history]
    else:
        out= [vid, new]
    return "\t".join(out)


def sorted_item(line):
    vid, items = line
    item_out=[]
    for timeStamp, item in items:
        try:
            timeStamp = datetime.strptime(timeStamp, "%Y-%m-%d %H:%M:%S.%f")
        except Exception:
            timeStamp = datetime.now()
        finally:
            item_out.append((timeStamp, item))
    items = sorted(item_out, key=lambda x: x[0])
    item_ft = items[0][1]
    return [vid, item_ft]


def sorted_item_bak(line):
    vid, items = line
    items = sorted(items, key=lambda x: x[0])
    item_ft = items[-1][1]
    return [vid, item_ft]



if __name__ == "__main__":
    main()

#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 80 --executor-cores 4 --executor-memory 8g /home/hadoop/users/jcx/hive/everyday_stat/build_dataset.py --start_day 2019-09-01 --end_day 2019-09-02 --save_date 2019-09-02