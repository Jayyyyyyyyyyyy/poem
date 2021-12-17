# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/8 2:35 PM
# @File      : recall_content_mp3_teacher.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import json
import sys
from pyspark import SparkContext
from collections import Counter

if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    date = sys.argv[1]
    sc = SparkContext(appName = "jcx")

    outpath = '/home/hadoop/users/jcx/hive/content_teacher_recall/result_{}.txt'.format(date)
    path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format(date)
    alldata = sc.textFile(path)
    all_video_mp3_name = alldata.map(lambda x: x.split('\t')[1]).filter(lambda x: 'profileinfo' in json.loads(x, encoding='utf-8')).\
        filter(lambda x: 'video_mp3_name' in json.loads(x, encoding='utf-8')['profileinfo']).\
        map(lambda x: json.loads(x, encoding='utf-8')['profileinfo']['video_mp3_name'])

    all_content_teacher = alldata.map(lambda x: x.split('\t')[1]).filter(lambda x: 'profileinfo' in json.loads(x, encoding='utf-8')).\
        filter(lambda x: 'content_teacher' in json.loads(x, encoding='utf-8')['profileinfo']).\
        filter(lambda x: 'tagname' in json.loads(x, encoding='utf-8')['profileinfo']['content_teacher']).\
        map(lambda x: json.loads(x, encoding='utf-8')['profileinfo']['content_teacher']['tagname'])

    all_content_mp3 = alldata.map(lambda x: x.split('\t')[1]).filter(
        lambda x: 'profileinfo' in json.loads(x, encoding='utf-8')). \
        filter(lambda x: 'content_mp3' in json.loads(x, encoding='utf-8')['profileinfo']). \
        filter(lambda x: 'tagname' in json.loads(x, encoding='utf-8')['profileinfo']['content_mp3']). \
        map(lambda x: json.loads(x, encoding='utf-8')['profileinfo']['content_mp3']['tagname'])

    # cutted_df = all_content_mp3.map(big_json)
    # print('saving')
    #all_content_mp3.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/content_mp3")
    #all_video_mp3_name.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/video_mp3_name")
    #all_content_teacher.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/content_teacher")
    tmp = all_content_mp3.collect()
    mylist = Counter(tmp)
    dici = dict(mylist)
    tmp2 = []
    for key in dici.keys():
        tmp2.append((key, dici[key]))

    tmp2 = sorted(tmp2, key=lambda tup: tup[1], reverse=True)
    with open('./content_mp3_with_c','w') as writer:
        for e in tmp2:
            newline = "{}\t{}\n".format(e[0], e[1])
            writer.write(newline)


    #day
    #today_total = alldata.filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(lambda x: json.loads(x, encoding='utf-8')['videoinfo']['createtime'][:10] == date)
    #remove_dup = alldata.map(lambda x: (json.loads(x, encoding='utf-8')['id'], [json.loads(x, encoding='utf-8')['utime'], x]))
    #keep_latest_utime = remove_dup.groupByKey().map(func)




# /home/hadoop/spark/bin/spark-submit --queue root.spark --master yarn   --num-executors 100 --executor-cores 4 --executor-memory 4g /home/hadoop/users/jcx/hive/content_teacher_recall/recall_teacher.py 2019-08-30
# execute command
#
# import json
# import pickle
# date = '2019-08-27'
# #path = "/video_profile/{}/*/*".format(date)
# path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format(date)
# alldata = sc.textFile(path)
# teacher_name_path = '/home/hadoop/users/jcx/hive/content_teacher_recall/teacher_name.pickle'
# teacher_names = pickle.load(open(teacher_name_path, 'rb'))
# def func(lines):
#     key = lines[0]
#     value = lines[1]
#     org = sorted(value,reverse=True)[0][1]
#     return org
#
# def find_teacher(row):
#     global  teacher_names
#     title = json.loads(row, encoding='utf-8')['videoinfo']['title']
#     for name in teacher_names:
#         if name in  title:
#             return True
#         else:
#             pass
#     return False
#
#
# alldata = alldata.map(lambda x: x.split('\t')[1]).filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(lambda x: 'profileinfo' in json.loads(x, encoding='utf-8')).filter(lambda x: 'content_teacher' in json.loads(x, encoding='utf-8')['profileinfo']).filter(lambda x: 'segment' in json.loads(x, encoding='utf-8')['profileinfo'])
#
# today_total = alldata.filter(lambda x: 'videoinfo' in json.loads(x, encoding='utf-8')).filter(lambda x : json.loads(x, encoding='utf-8')['videoinfo']['createtime'][:10]==date)
# remove_dup = today_total.map(lambda x: (json.loads(x, encoding='utf-8')['id'], [json.loads(x, encoding='utf-8')['utime'], x]))
# keep_latest_utime = remove_dup.groupByKey().map(func)
# empty_teacher_tagname = keep_latest_utime.filter(lambda x: json.loads(x, encoding='utf-8')['profileinfo']['content_teacher'] == {})
# has_teacher_tagname = keep_latest_utime.filter(lambda x : 'tagname' in json.loads(x, encoding='utf-8')['profileinfo']['content_teacher'])
# has_teacher_tagname_unknown = has_teacher_tagname.filter(lambda x: (json.loads(x, encoding='utf-8')['profileinfo']['content_teacher']['tagname']=='unknown'))
#
# get_result1 = empty_teacher_tagname.filter(find_teacher).map(lambda x: (json.loads(x, encoding='utf-8')['id'], json.loads(x, encoding='utf-8')['videoinfo']['title'])).collect()
# get_result2 = has_teacher_tagname_unknown.filter(find_teacher).map(lambda x: (json.loads(x, encoding='utf-8')['id'], json.loads(x, encoding='utf-8')['videoinfo']['title'])).collect()

