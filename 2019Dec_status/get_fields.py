# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/20 4:48 PM
# @File      : get_fields.py
# @Software  : PyCharm
# @Company   : Xiao Tang



import json
import sys
import requests
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import *
from collections import OrderedDict
import re





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
        if int(row['cstatus'] )==0:
            if 'videoinfo' in row and 'profileinfo' in row:
                return True
            else:
                return False
        else:
            return False
    else:
        return False

def parse_id(row):
    row = row[1]
    id = row.split('\t')[0]
    return int(id)

def filter_parse_row(row):
    ids2 = set(['2089','2090','2091','2092','2093','2094','1889','1890','1891','1892','1897','1898','1899','1900','1905','1906','1907','1908'])
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    try:
        if row['cstatus'] == 0 and row['ctype'] == [101, 102, 103, 105, 106, 107, 301, 121] and row['cstage'] == [6,7,8,10]:
            if 'profileinfo' in row:
                if 'content_phrase' in row['profileinfo']:
                    if 'tagid' in row['profileinfo']['content_phrase']:
                        if str(row['profileinfo']['content_phrase']['tagid']) in ids2:
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    return False
            else:
                return False
        else:
            return False
    except:
        return False

def filter_parse_row2(row):
    row = row[1]
    id = row.split('\t')[0]
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    status = row['cstatus']
    try:
        video_mp3_name = row['profileinfo']['video_mp3_name']
        title = row['videoinfo']['title']
        uname = row['profileinfo']['uname']
        uname_seg = row['profileinfo']['uname_seg']
        segment = row['profileinfo']['segment']
        content_mp3_seg = row['profileinfo']['content_mp3_seg']
        uid_team_name_seg = row['profileinfo']['uid_team_name_seg']
        ocr_text_seg = row['profileinfo']['ocr_text_seg']
        return False
    except:
        return True

def parse_row2(row):
    row = row[1]
    id = row.split('\t')[0]
    id = re.sub("[^0-9]+", "", id)
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    content_phrase_tagid = row['profileinfo']['content_phrase']['tagid']
    return (int(id), content_phrase_tagid)



def parse_row(row):
    row = row[1]
    id = row.split('\t')[0]
    id = re.sub("[^0-9]+", "", id)
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


def table_schema():
    col_Dict = OrderedDict()
    col_Dict['vid'] = StructField('vid', LongType(), True)
    col_Dict['title'] = StructField('title', StringType(), True)
    col_Dict['uname'] = StructField('uname', StringType(), True)
    col_Dict['video_mp3_name'] = StructField('video_mp3_name', StringType(), True)
    col_Dict['uid_team_name'] = StructField('uid_team_name', StringType(), True)
    col_Dict['ocr_text'] = StructField('ocr_text', StringType(), True)
    col_Dict['segment'] = StructField('segment', StringType(), True)
    col_Dict['uname_seg'] = StructField('uname_seg', StringType(), True)
    col_Dict['content_mp3_seg'] = StructField('content_mp3_seg', StringType(), True)
    col_Dict['uid_team_name_seg'] = StructField('uid_team_name_seg', StringType(), True)
    col_Dict['ocr_text_seg'] = StructField('ocr_text_seg', StringType(), True)
    col_Dict['mp3name'] = StructField('mp3name', StringType(), True)
    schema = StructType(list(col_Dict.values()))
    return schema

def to_json(vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name):
    tmp = {}
    tmp['vid'] = vid
    tmp['title'] = title
    tmp['uname'] = uname
    tmp['video_mp3_name'] = video_mp3_name
    tmp['uid_team_name'] = uid_team_name
    tmp['ocr_text'] = ocr_text
    tmp['segment'] = segment
    tmp['uname_seg'] = uname_seg
    tmp['content_mp3_seg'] = content_mp3_seg
    tmp['uid_team_name_seg'] = uid_team_name_seg
    tmp['ocr_text_seg'] = ocr_text_seg
    tmp['content_mp3'] = mp3name
    final_res = json.dumps(tmp, ensure_ascii=False)
    return final_res





if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')

    sc = SparkContext(appName="test")
    yesterday_path = "hdfs://10.42.4.78:8020/user/jiaxj/Apps/DocsInfo/{}/*".format(yesterday)
    yesterday_data = sc.newAPIHadoopFile(yesterday_path, "com.hadoop.mapreduce.LzoTextInputFormat", "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")

    # incomplete_ids = yesterday_data.filter(filter_cstatus).filter(filter_parse_row2).map(parse_id)
    # ids = "/user/jiangcx/his_diff_vids/ids_{}/".format(yesterday)
    # incomplete_ids.saveAsTextFile(ids)
    yesterday_data = yesterday_data.filter(filter_parse_row).map(parse_row2)
    total_data = "/user/jiangcx/his_diff_vids/can_delete_{}/".format(yesterday)
    yesterday_data.saveAsTextFile(total_data)
#     spark = SparkSession.builder \
#         .appName('user_long_interest') \
#         .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
#         .enableHiveSupport().getOrCreate()
#
#
#     df2rdd = yesterday_data.map(lambda x: Row(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11]))
#     # df_final = df2rdd.toDF(['vid', 'videourl', 'result'])
#     df = spark.createDataFrame(df2rdd, table_schema())
#     df.createOrReplaceTempView("hdfs_field_table")
#
#
#     sql = "select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name from hdfs_field_table where length(trim(title))<>0"
#     sqldf = spark.sql(sql)
#     to_save = sqldf.rdd.map(lambda x: to_json(x.vid, x.title.encode('utf8'), x.uname.encode('utf8'), x.video_mp3_name.encode('utf8'), x.uid_team_name.encode('utf8'), x.ocr_text.encode('utf8'), x.segment.encode('utf8'), x.uname_seg.encode('utf8'), x.content_mp3_seg.encode('utf8'), x.uid_team_name_seg.encode('utf8'), x.ocr_text_seg.encode('utf8'), x.mp3name.encode('utf8')))
#     total_data = "/user/jiangcx/his_diff_vids/total_data_{}/".format(yesterday)
#     to_save.saveAsTextFile(total_data)
#
#     spark.stop()
#
# import pandas as pd
# pd.DataFrame()



    # vids = df.select('vid','status').collect()







#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_status_check/get_fields.py 2019-12-19 > running.log 2>&1

