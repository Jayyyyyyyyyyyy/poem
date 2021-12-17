# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/30 7:28 PM
# @File      : stat_of_fields.py
# @Software  : PyCharm
# @Company   : Xiao Tang

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

    parse_data = yesterday_data.map(lambda  x: json.loads(x[1].split('\t')[1], encoding='utf-8'))

    ###### tmp
    my = parse_data.filter(lambda x: 'profileinfo' in x).filter(lambda x: 'content_mp3' in x).filter(lambda x: 'tagname' in x['content_mp3']).filter(
        lambda x: x['content_mp3']['tagname'] == '++').map(lambda x: x['id'])
    ######

    all_cnt = parse_data.count()
    has_id = parse_data.filter(lambda x: 'id' in x).count()
    has_id_str = "id: {}-{}={}".format(all_cnt, has_id, all_cnt - has_id)
    has_ctype = parse_data.filter(lambda x: 'ctype' in x).count()
    has_ctype_str = "ctype: {}-{}={}".format(all_cnt, has_ctype, all_cnt - has_ctype)
    has_cstatus = parse_data.filter(lambda x: 'cstatus' in x).count()
    has_cstatus_str = "cstatus: {}-{}={}".format(all_cnt, has_cstatus, all_cnt - has_cstatus)
    has_cstage = parse_data.filter(lambda x: 'cstage' in x).count()
    has_cstage_str = "cstage: {}-{}={}".format(all_cnt, has_cstage, all_cnt - has_cstage)

    has_utime = parse_data.filter(lambda x: 'utime' in x).count()
    has_utime_str = "utime: {}-{}={}".format(all_cnt, has_utime, all_cnt - has_utime)
    has_etime = parse_data.filter(lambda x: 'etime' in x).count()
    has_etime_str = "etime: {}-{}={}".format(all_cnt, has_etime, all_cnt - all_cnt)
    has_ctime = parse_data.filter(lambda x: 'ctime' in x).count()
    has_ctime_str = "ctime: {}-{}={}".format(all_cnt, has_ctime, all_cnt - has_ctime)
    has_ptime = parse_data.filter(lambda x: 'ptime' in x).count()
    has_ptime_str = "ptime: {}-{}={}".format(all_cnt, has_ptime, all_cnt - has_ptime)
    has_rtime = parse_data.filter(lambda x: 'rtime' in x).count()
    has_rtime_str = "rtime: {}-{}={}".format(all_cnt, has_rtime, all_cnt - has_rtime)

    tmp_profileinfo = parse_data.filter(lambda x: 'profileinfo' in x)
    has_profileinfo = tmp_profileinfo.count()
    has_profileinfo_str = "profileinfo: {}-{}={}".format(all_cnt, has_profileinfo, all_cnt - has_profileinfo)

    ###profileinfo
    profileinfo_obj = tmp_profileinfo.map(lambda x: x['profileinfo'])

    ###second level
    has_content_mp3 = profileinfo_obj.filter(lambda x: 'content_mp3' in x).count()

    has_content_mp3_str = "content_mp3: {}-{}={}".format( has_profileinfo, has_content_mp3, has_profileinfo - has_content_mp3)

    has_segment = profileinfo_obj.filter(lambda x: 'segment' in x).count()
    has_segment_str = "segment: {}-{}={}".format(has_profileinfo, has_segment, has_profileinfo - has_segment)

    has_video_mp3_name = profileinfo_obj.filter(lambda x: 'video_mp3_name' in x).count()
    has_video_mp3_name_str = "video_mp3_name: {}-{}={}".format(has_profileinfo, has_video_mp3_name, has_profileinfo - has_video_mp3_name)

    has_uid_team_name = profileinfo_obj.filter(lambda x: 'uid_team_name' in x).count()
    has_uid_team_name_str = "uid_team_name: {}-{}={}".format(has_profileinfo, has_uid_team_name, has_profileinfo - has_uid_team_name)

    has_ocr_text = profileinfo_obj.filter(lambda x: 'ocr_text' in x).count()
    has_ocr_text_str = "ocr_text: {}-{}={}".format(has_profileinfo, has_ocr_text, has_profileinfo - has_ocr_text)

    has_uname = profileinfo_obj.filter(lambda x: 'uname' in x).count()
    has_uname_str = "uname: {}-{}={}".format(has_profileinfo, has_uname, has_profileinfo - has_uname)

    has_uname_seg = profileinfo_obj.filter(lambda x: 'uname_seg' in x).count()
    has_uname_seg_str = "uname_seg: {}-{}={}".format(has_profileinfo, has_uname_seg, has_profileinfo - has_uname_seg)

    has_content_mp3_seg = profileinfo_obj.filter(lambda x: 'content_mp3_seg' in x).count()
    has_content_mp3_seg_str = "content_mp3_seg: {}-{}={}".format(has_profileinfo, has_content_mp3_seg, has_profileinfo - has_content_mp3_seg)

    has_uid_team_name_seg = profileinfo_obj.filter(lambda x: 'uid_team_name_seg' in x).count()
    has_uid_team_name_seg_str = "uid_team_name_seg: {}-{}={}".format(has_profileinfo, has_uid_team_name_seg, has_profileinfo - has_uid_team_name_seg)

    has_ocr_text_seg = profileinfo_obj.filter(lambda x: 'ocr_text_seg' in x).count()
    has_ocr_text_seg_str = "ocr_text_seg: {}-{}={}".format(has_profileinfo, has_ocr_text_seg, has_profileinfo - has_ocr_text_seg)


    ###videoinfo
    tmp_videoinfo = parse_data.filter(lambda x: 'videoinfo' in x)
    has_videoinfo = tmp_videoinfo.count()
    has_videoinfo_str = "videoinfo: {}-{}={}".format(all_cnt, has_videoinfo, all_cnt - has_videoinfo)
    videoinfo_obj = tmp_videoinfo.map(lambda x: x['videoinfo'])
    has_title = videoinfo_obj.filter(lambda x: 'title' in x).count()
    has_title_str = "title: {}-{}={}".format(has_videoinfo, has_title, has_videoinfo - has_title)



    outpath2 = '/home/hadoop/users/jcx/hive/his_status_check/history_key_fields_stats_{}.txt'.format(yesterday)
    with open(outpath2, 'w') as w:
        w.write(has_id_str + '\n')
        w.write(has_ctype_str + '\n')
        w.write(has_cstatus_str + '\n')
        w.write(has_cstage_str + '\n')
        w.write(has_utime_str + '\n')
        w.write(has_etime_str + '\n')
        w.write(has_ctime_str + '\n')
        w.write(has_ptime_str + '\n')
        w.write(has_rtime_str + '\n')
        w.write(has_profileinfo_str + '\n')
        w.write(has_videoinfo_str + '\n')

        w.write('\n')
        w.write('\n')
        w.write(has_content_mp3_str + '\n')
        w.write(has_segment_str + '\n')
        w.write(has_video_mp3_name_str + '\n')
        w.write(has_uid_team_name_str + '\n')
        w.write(has_ocr_text_str + '\n')
        w.write(has_uname_str + '\n')
        w.write(has_uname_seg_str + '\n')
        w.write(has_content_mp3_seg_str + '\n')
        w.write(has_uid_team_name_seg_str + '\n')
        w.write(has_ocr_text_seg_str + '\n')
        w.write(has_title_str + '\n')


#/home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 60 --executor-cores 4 --executor-memory 24g /home/hadoop/users/jcx/hive/his_status_check/stat_of_fields.py 2019-12-29 > running.log 2>&1
