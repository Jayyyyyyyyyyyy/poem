# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/30 7:28 PM
# @File      : imcomplete_data.py
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



def filter_all_incomplete_profile(jsonobj):
    if 'id' not in jsonobj or 'ctype' not in jsonobj or 'cstatus' not in jsonobj or 'cstage' not in jsonobj or 'utime' not in jsonobj or 'etime' not in jsonobj or 'ctime' not in jsonobj or 'ptime' not in jsonobj or 'rtime' not in jsonobj or 'profileinfo' not in jsonobj or 'videoinfo' not in jsonobj:
        return True
    else:
        return False




def parse_row(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    if 'video_mp3_name' in row['profileinfo']:
        video_mp3_name = row['profileinfo']['video_mp3_name']
    else:
        video_mp3_name = ''
    if 'title' in row['videoinfo']:
        title = row['videoinfo']['title']
    else:
        title = ''

    if 'uid_team_name' in row['profileinfo']:
        uid_team_name = row['profileinfo']['uid_team_name']
    else:
        uid_team_name = ''


    if 'ocr_text' in row['profileinfo']:
        ocr_text = row['profileinfo']['ocr_text']
    else:
        ocr_text = ''

    if 'uname' in row['profileinfo']:
        uname = row['profileinfo']['uname']
    else:
        uname = ''

    if 'uname_seg' in row['profileinfo']:
        uname_seg = row['profileinfo']['uname_seg']
    else:
        uname_seg = ''

    if 'segment' in row['profileinfo']:
        segment = row['profileinfo']['segment']
    else:
        segment = ''

    if 'content_mp3_seg' in row['profileinfo']:
        content_mp3_seg = row['profileinfo']['content_mp3_seg']
    else:
        content_mp3_seg = ''

    if 'uid_team_name_seg' in row['profileinfo']:
        uid_team_name_seg = row['profileinfo']['uid_team_name_seg']
    else:
        uid_team_name_seg = ''
    if 'ocr_text_seg' in row['profileinfo']:
        ocr_text_seg = row['profileinfo']['ocr_text_seg']
    else:
        ocr_text_seg = ''
    if 'content_mp3' in row['profileinfo']:
        content_mp3 = row['profileinfo']['content_mp3']
        if 'tagname' in row['profileinfo']['content_mp3']:
            mp3name = row['profileinfo']['content_mp3']['tagname']
        else:
            mp3name = ''
    else:
        mp3name = ''

    return (int(id), title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name)


# def filter_segs(jsonobj):
#     if 'title' not in jsonobj['videoinfo'] or 'uname' not in jsonobj['profileinfo'] or

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')


    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')

    sc = SparkContext(appName="test")
    yesterday_path = "hdfs://10.42.4.78:8020/user/jiaxj/Apps/DocsInfo/{}/*".format(yesterday)
    yesterday_data = sc.newAPIHadoopFile(yesterday_path, "com.hadoop.mapreduce.LzoTextInputFormat","org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")

    parse_data = yesterday_data.map(lambda  x: json.loads(x[1].split('\t')[1], encoding='utf-8'))
    res = parse_data.filter(filter_all_incomplete_profile).map(lambda x: x['id'])
    res.repartition(1).saveAsTextFile("/user/jiangcx/incomplete_data_{}".format(yesterday))


#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_status_check/incomplete_data.py 2019-12-30 > running.log 2>&1

