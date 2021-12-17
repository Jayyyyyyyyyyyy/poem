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
from pyspark.sql.types import *
from collections import OrderedDict
from datetime import datetime
import time
import re


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
        if 'tagname' in row['profileinfo']['content_mp3']:
            mp3name = row['profileinfo']['content_mp3']['tagname']
        else:
            mp3name = ''
    else:
        mp3name = ''
    return (int(id), title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name)

def parse_row_es(row):
    id = row['id']
    if 'video_mp3_name' in row:
        video_mp3_name = row['video_mp3_name']
    else:
        video_mp3_name = ''
    if 'title' in row:
        title = row['title']
    else:
        title = ''
    if 'uid_team_name' in row:
        uid_team_name = row['uid_team_name']
    else:
        uid_team_name = ''
    if 'ocr_text' in row:
        ocr_text = row['ocr_text']
    else:
        ocr_text = ''

    if 'uname' in row:
        uname = row['uname']
    else:
        uname = ''

    if 'uname_seg' in row:
        # uname_seg = row['uname_seg']
        # uname_seg = " ".join(["{}/{}".format(x['name'].decode('utf8'), x['weight']) for x in uname_seg])
        uname_seg = ''
    else:
        uname_seg = ''

    if 'segment' in row:
        segment = row['segment']
    else:
        segment = ''

    if 'content_mp3_seg' in row:
        # content_mp3_seg = row['content_mp3_seg']
        # content_mp3_seg = " ".join(["{}/{}".format(x['name'].decode('utf8'), x['weight'])  for x in content_mp3_seg])
        content_mp3_seg = ''
    else:
        content_mp3_seg = ''

    if 'uid_team_name_seg' in row:
        # uid_team_name_seg = row['uid_team_name_seg']
        # uid_team_name_seg = " ".join(["{}/{}".format(x['name'].decode('utf8'), x['weight']) for x in uid_team_name_seg])
        uid_team_name_seg = ''
    else:
        uid_team_name_seg = ''
    if 'ocr_text_seg' in row:
        # ocr_text_seg = row['ocr_text_seg']
        # ocr_text_seg = " ".join(["{}/{}".format(x['name'].decode('utf8'), x['weight']) for x in ocr_text_seg])
        ocr_text_seg = ''
    else:
        ocr_text_seg = ''

    if 'content_mp3' in row:
        if 'tagname' in row['content_mp3']:
            mp3name = row['content_mp3']['tagname']
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

def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    esvids = []
    c = 0
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['id', 'title', 'uname', 'video_mp3_name', 'uid_team_name', 'ocr_text', 'segment', 'uname_seg', 'content_mp3_seg', 'uid_team_name_seg', 'ocr_text_seg','content_mp3']}

        payload = json.dumps(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        reses = []
        json1 = response.json()['hits']['hits']
        for x in json1:
            res = x['_source']
            line = parse_row_es(res)
            reses.append(line)
        esvids.extend(reses)
    return esvids


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

if __name__ == '__main__':
    start = time.time()
    input = sys.argv[1]
    output = sys.argv[2]
    prefix = sys.argv[3]
    fromDate = sys.argv[4]
    toDate = sys.argv[5]
    reload(sys)
    sys.setdefaultencoding('utf-8')

    sc = SparkContext(appName="test")

    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()

    sql = "select vid from da.video_cstage where dt>={} and dt<={} and cstage in (6,7,8,10)".format(fromDate, toDate)
    spark.sql(sql).rdd.map(lambda r: (str(r.vid), 1)).collectAsMap()

    dateobj = datetime.strptime(fromDate, '%Y-%m-%d')
    t = dateobj.timetuple()
    fromDateMilli = int(time.mktime(t))*1000

    # path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format(date)
    input = "/user/wangshuang/apps/xdata/user_history/dt=2020-09-06"
    rdd = sc.textFile(input).flatMap(lambda line: line.split('\t')) # TODO 解析并且过滤


    # sql = "select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name from hdfs_field_table where length(trim(title))<>0"
    # sqldf = spark.sql(sql)
    # to_save = sqldf.rdd.map(lambda x: to_json(x.vid, x.title.encode('utf8'), x.uname.encode('utf8'), x.video_mp3_name.encode('utf8'), x.uid_team_name.encode('utf8'), x.ocr_text.encode('utf8'), x.segment.encode('utf8'), x.uname_seg.encode('utf8'), x.content_mp3_seg.encode('utf8'), x.uid_team_name_seg.encode('utf8'), x.ocr_text_seg.encode('utf8'), x.mp3name.encode('utf8')))
    # miss_vids_sql = "select db.vid from (select vid from hdfs_field_table where length(trim(title))<>0)hdfs  right join (select vid from dw.video where status=0 and length(trim(title))<>0)db on(hdfs.vid = db.vid) where hdfs.vid is null"
    # miss_vids = spark.sql(miss_vids_sql)
    # vids = miss_vids.rdd.map(lambda x: x.vid).collect()
    # results = eschecker(vids)
    # out_path = '/home/hadoop/users/jcx/hive/his_content_mp3_update_all/fill_up_vids_{}'.format(yesterday)
    # with open(out_path,'w') as w:
    #     for vid in vids:
    #         line = "{}\n".format(vid)
    #         w.write(line)
    # miss_vids_rdd = sc.parallelize(results)
    # miss_df2rdd = miss_vids_rdd.map(lambda x: Row(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11]))
    # miss_df = spark.createDataFrame(miss_df2rdd, table_schema())
    # miss_df.createOrReplaceTempView("miss_table")
    # miss_sql = "select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg, mp3name from (select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg, mp3name from hdfs_field_table where length(trim(title))<>0 union all select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg, mp3name from miss_table)a"
    # miss_sqldf = spark.sql(miss_sql)
    # miss_to_save = miss_sqldf.rdd.map(
    #     lambda x: to_json(x.vid, x.title.encode('utf8'), x.uname.encode('utf8'), x.video_mp3_name.encode('utf8'),
    #                       x.uid_team_name.encode('utf8'), x.ocr_text.encode('utf8'), x.segment.encode('utf8'),
    #                       x.uname_seg.encode('utf8'), x.content_mp3_seg.encode('utf8'),
    #                       x.uid_team_name_seg.encode('utf8'), x.ocr_text_seg.encode('utf8'), x.mp3name.encode('utf8')))
    # total_data = "/user/jiangcx/his_diff_vids/total_data_{}/".format(yesterday)
    # miss_to_save.saveAsTextFile(total_data)
    # spark.stop()





#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_content_mp3_update_all/get_field_es.py 2020-01-08 > running.log 2>&1

