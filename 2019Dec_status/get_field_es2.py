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
import time
import re
import os
os.environ['PYSPARK_PYTHON']='/usr/local/bin/python'



def parse_row(row):
    row = row[1]
    id = row.split('\t')[0]
    id = re.sub("[^0-9]+", "", id)
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    ctime = row['ctime']
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
    if 'segment_def' in row['profileinfo']:
        segment_def = row['profileinfo']['segment_def']
    else:
        segment_def = ''
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
    if 'talentstar' in row['profileinfo']:
        talentstar = str(row['profileinfo']['talentstar'])
    else:
        talentstar = ''
    if 'uid' in row['videoinfo']:
        uid = str(row['videoinfo']['uid'])
    else:
        uid = ''
    if 'content_teacher' in row['profileinfo']:
        if 'tagname' in row['profileinfo']['content_teacher']:
            content_teacher_tagname = row['profileinfo']['content_teacher']['tagname']
        else:
            content_teacher_tagname = ''
        if 'tagvalue' in row['profileinfo']['content_teacher']:
            content_teacher_tagvalue = int(row['profileinfo']['content_teacher']['tagvalue'])
        else:
            content_teacher_tagvalue = -1
    else:
        content_teacher_tagname = ''
        content_teacher_tagvalue = -1
    if 'firstcat' in row['profileinfo']:
        if 'tagid' in row['profileinfo']['firstcat']:
            firstcat_tagid = int(row['profileinfo']['firstcat']['tagid'])
        else:
            firstcat_tagid = -1
    else:
        firstcat_tagid = -1
    return (int(id), title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid)


def parse_row_es(row):
    id = row['id']
    ctime = row['ctime']
    if 'video_mp3_name' in row:
        video_mp3_name = row['video_mp3_name']
    else:
        video_mp3_name = ''
    if 'title' in row:
        title = row['title']
    else:
        title = ''

    if 'uid' in row:
        uid = str(row['uid'])
    else:
        uid = ''

    if 'talentstar' in row:
        talentstar = str(row['talentstar'])
    else:
        talentstar = ''

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

    if 'segment_def' in row:
        segment_def = row['segment_def']
    else:
        segment_def = ''

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

    if 'content_teacher' in row:
        if 'tagname' in row['content_teacher']:
            content_teacher_tagname = row['content_teacher']['tagname']
        else:
            content_teacher_tagname = ''
        if 'tagvalue' in row['content_teacher']:
            content_teacher_tagvalue = int(row['content_teacher']['tagvalue'])
        else:
            content_teacher_tagvalue = -1
    else:
        content_teacher_tagname = ''
        content_teacher_tagvalue = -1

    if 'firstcat' in row:
        if 'tagid' in row['firstcat']:
            firstcat_tagid = int(row['firstcat']['tagid'])
        else:
            firstcat_tagid = -1
    else:
        firstcat_tagid = -1

    return (int(id), title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid)


def table_schema():
    col_Dict = OrderedDict()
    col_Dict['vid'] = StructField('vid', LongType(), True)
    col_Dict['title'] = StructField('title', StringType(), True)
    col_Dict['uname'] = StructField('uname', StringType(), True)
    col_Dict['video_mp3_name'] = StructField('video_mp3_name', StringType(), True)
    col_Dict['uid_team_name'] = StructField('uid_team_name', StringType(), True)
    col_Dict['ocr_text'] = StructField('ocr_text', StringType(), True)
    col_Dict['segment'] = StructField('segment', StringType(), True)
    col_Dict['segment_def'] = StructField('segment_def', StringType(), True)
    col_Dict['uname_seg'] = StructField('uname_seg', StringType(), True)
    col_Dict['content_mp3_seg'] = StructField('content_mp3_seg', StringType(), True)
    col_Dict['uid_team_name_seg'] = StructField('uid_team_name_seg', StringType(), True)
    col_Dict['ocr_text_seg'] = StructField('ocr_text_seg', StringType(), True)
    col_Dict['mp3name'] = StructField('mp3name', StringType(), True)
    col_Dict['talentstar'] = StructField('talentstar', StringType(), True)
    col_Dict['uid'] = StructField('uid', StringType(), True)
    col_Dict['content_teacher_tagname'] = StructField('content_teacher_tagname', StringType(), True)
    col_Dict['content_teacher_tagvalue'] = StructField('content_teacher_tagvalue', IntegerType(), True)
    col_Dict['ctime'] = StructField('ctime', LongType(), True)
    col_Dict['firstcat_tagid'] = StructField('firstcat_tagid', IntegerType(), True)
    schema = StructType(list(col_Dict.values()))
    return schema

def to_json(vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def,uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid):
    tmp = {}
    tmp['vid'] = vid
    tmp['title'] = title
    tmp['uname'] = uname
    tmp['video_mp3_name'] = video_mp3_name
    tmp['uid_team_name'] = uid_team_name
    tmp['ocr_text'] = ocr_text
    tmp['segment'] = segment
    tmp['segment_def'] = segment_def
    tmp['uname_seg'] = uname_seg
    tmp['content_mp3_seg'] = content_mp3_seg
    tmp['uid_team_name_seg'] = uid_team_name_seg
    tmp['ocr_text_seg'] = ocr_text_seg
    tmp['content_mp3'] = mp3name
    tmp['talentstar'] = talentstar
    tmp['uid'] = uid
    tmp['content_teacher_tagname'] = content_teacher_tagname
    tmp['content_teacher_tagvalue'] = content_teacher_tagvalue
    tmp['ctime'] = ctime
    tmp['firstcat_tagid'] = firstcat_tagid
    final_res = json.dumps(tmp, ensure_ascii=False)
    return final_res

def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    esvids = []
    c = 0
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['id', 'title', 'uname', 'video_mp3_name', 'uid_team_name', 'ocr_text', 'segment', 'uname_seg', 'content_mp3_seg', 'uid_team_name_seg', 'ocr_text_seg','content_mp3', 'talentstar', 'uid','content_teacher', 'ctime', 'firstcat']}

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
    id = re.sub("[^0-9]+", "", id)
    if id == '':
        return False
    row = json.loads(row.split('\t')[1], encoding='utf-8')
    if 'cstatus' in row:
        if int(row['cstatus'] ) in (0,6,7,8):
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
    reload(sys)
    sys.setdefaultencoding('utf-8')

    yesterday = sys.argv[1]
    ip = sys.argv[2]
    yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
    yesterday_2 = yesterday_object.strftime('%Y%m%d')
    # ip = '10.42.178.9:8020'
    # yesterday = '2021-03-26'
    sc = SparkContext(appName="get_field_es2py")
    yesterday_path = "hdfs://{}/user/jiaxj/Apps/DocsInfo/{}/*".format(ip, yesterday)
    yesterday_data = sc.newAPIHadoopFile(yesterday_path, "com.hadoop.mapreduce.LzoTextInputFormat",
                                         "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
    yesterday_data = yesterday_data.filter(filter_cstatus)

    # incomplete_ids = yesterday_data.filter(filter_cstatus).filter(filter_parse_row2).map(parse_id)
    # ids = "/user/jiangcx/his_diff_vids/ids_{}/".format(yesterday)
    # incomplete_ids.saveAsTextFile(ids)
    spark = SparkSession.builder \
        .appName('user_long_interest') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()

    yesterday_data = yesterday_data.filter(filter_cstatus).map(parse_row)
    df2rdd = yesterday_data.map(lambda x: Row(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18]))
    df = spark.createDataFrame(df2rdd, table_schema())
    df.createOrReplaceTempView("hdfs_field_table")

    sql = "select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg,mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid from hdfs_field_table where length(trim(title))<>0"
    sqldf = spark.sql(sql)
    to_save = sqldf.rdd.map(lambda x: to_json(x.vid, x.title.encode('utf8'), x.uname.encode('utf8'), x.video_mp3_name.encode('utf8'), x.uid_team_name.encode('utf8'), x.ocr_text.encode('utf8'), x.segment.encode('utf8'), x.segment_def.encode('utf8'), x.uname_seg.encode('utf8'), x.content_mp3_seg.encode('utf8'), x.uid_team_name_seg.encode('utf8'), x.ocr_text_seg.encode('utf8'), x.mp3name.encode('utf8'), x.talentstar.encode('utf8'), x.uid.encode('utf8'), x.content_teacher_tagname.encode('utf8'), x.content_teacher_tagvalue, x.ctime, x.firstcat_tagid))

    miss_vids_sql = "select db.vid from (select vid from hdfs_field_table where length(trim(title))<>0)hdfs  right join (select vid from dw.video where status=0 and length(trim(title))<>0)db on(hdfs.vid = db.vid) where hdfs.vid is null"
    miss_vids = spark.sql(miss_vids_sql)

    vids = miss_vids.rdd.map(lambda x: x.vid).collect()
    results = eschecker(vids)
    print("line261")
    print(len(results))
    out_path = '/data/jiangcx/workspace/hive/his_content_mp3_update_all/fill_up_vids_{}'.format(yesterday)
    with open(out_path,'w') as w:
        for vid in vids:
            line = "{}\n".format(vid)
            w.write(line)


    miss_vids_rdd = sc.parallelize(results)
    miss_df2rdd = miss_vids_rdd.map(lambda x: Row(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15], x[16], x[17], x[18]))
    miss_df = spark.createDataFrame(miss_df2rdd, table_schema())
    miss_df.createOrReplaceTempView("miss_table")

    miss_sql = "select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg, mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid from (select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg, mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid from hdfs_field_table where length(trim(title))<>0 union all select vid, title, uname, video_mp3_name, uid_team_name, ocr_text, segment, segment_def, uname_seg, content_mp3_seg, uid_team_name_seg, ocr_text_seg, mp3name, talentstar, uid, content_teacher_tagname, content_teacher_tagvalue, ctime, firstcat_tagid from miss_table)a"
    miss_sqldf = spark.sql(miss_sql)
    miss_to_save = miss_sqldf.rdd.map(
        lambda x: to_json(x.vid, x.title.encode('utf8'), x.uname.encode('utf8'), x.video_mp3_name.encode('utf8'),
                          x.uid_team_name.encode('utf8'), x.ocr_text.encode('utf8'), x.segment.encode('utf8'), x.segment_def.encode('utf8'),
                          x.uname_seg.encode('utf8'), x.content_mp3_seg.encode('utf8'),
                          x.uid_team_name_seg.encode('utf8'), x.ocr_text_seg.encode('utf8'), x.mp3name.encode('utf8'), x.talentstar.encode('utf8'),
                          x.uid.encode('utf8'), x.content_teacher_tagname.encode('utf8'), x.content_teacher_tagvalue, x.ctime, x.firstcat_tagid))



    total_data = "/user/jiangcx/his_diff_vids_2/total_data_{}/".format(yesterday)
    miss_to_save.saveAsTextFile(total_data)
    spark.stop()





#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_content_mp3_update_all/get_field_es.py 2020-01-08 > running.log 2>&1


