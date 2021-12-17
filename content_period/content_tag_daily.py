# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/3/14 6:46 PM
# @File      : content_tag_daily.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import json
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
def func(lines):
    key = lines[0]
    value = lines[1]
    org = sorted(value,reverse=True)[0][1]
    return org


def parse_row(row):
    row = json.loads(row, encoding='utf-8')

    if 'cstatus' in row and 'cstage' in row:
        cstage = row['cstage']
        status = row['cstatus']
        if status == 0 and cstage in [6, 7, 8, 10]:
            if 'profileinfo' in row:
                if 'content_tag' in row['profileinfo']:
                    if len(row['profileinfo']['content_tag']) == 0:
                        return True

                    else:
                        return False
                    # return True
                else:
                    return False
            else:
                return False
        else:
            return False
    else:
        return False
if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    date = sys.argv[1]
    sc = SparkContext(appName = "test")

    outpath = '/home/hadoop/users/jcx/hive/permanent_notification_bar/stat_videoprofile/result_{}.txt'.format(date)
    path = "/video_profile/{}/*/*".format(date)

    alldata = sc.textFile(path)
    print("start today_total")

    today_total = alldata.filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(
        lambda x: 'createtime' in json.loads(x, encoding='utf-8')['videoinfo']).filter(
        lambda x: json.loads(x, encoding='utf-8')['videoinfo']['createtime'][:10] == date)
    remove_dup = today_total.map(
        lambda x: (json.loads(x, encoding='utf-8')['id'], [json.loads(x, encoding='utf-8')['utime'], x]))
    keep_latest_utime = remove_dup.groupByKey().map(func)

    vids_have_profile = keep_latest_utime.filter(parse_row)
    print(vids_have_profile.count())
#
#     with open(outpath,'w') as write:
#
#     # get vids from BigDataGroup
#
#
#
# spark = SparkSession.builder \
#     .appName('user_long_interest') \
#     .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
#     .enableHiveSupport().getOrCreate()
# #my_sql = "select vid from dw.video where to_date(createtime)={}".format(date)
# my_sql = "select vid from dw.video limit 10"
# spark.sql(my_sql).show()
