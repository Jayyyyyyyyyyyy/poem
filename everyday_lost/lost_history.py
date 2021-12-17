# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/8/31 6:24 PM
# @File      : lost.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/8 2:35 PM
# @File      : recall_content_mp3_teacher.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import json
import sys
from pyspark import SparkContext

if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    date = sys.argv[1]
    sc = SparkContext(appName = "jcx")

    outpath = '/home/hadoop/users/jcx/hive/content_teacher_recall/result_{}.txt'.format(date)
    path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format(date)
    alldata = sc.textFile(path)
    all_vid = alldata.map(lambda x: (x.split('\t')[0], x.split('\t')[1] ) ).filter(lambda x: 'profileinfo' in json.loads(x[1], encoding='utf-8')).\
        filter(lambda x: 'cstatus' in json.loads(x[1], encoding='utf-8')).filter(lambda x: json.loads(x[1], encoding='utf-8')['cstatus']==0 ).\
        map(lambda x: x[0])

    # cutted_df = all_content_mp3.map(big_json)
    # print('saving')
    all_vid.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/vid")



# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/8 2:35 PM
# @File      : recall_content_mp3_teacher.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import sys
from pyspark import SparkContext

if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    date = sys.argv[1]
    sc = SparkContext(appName = "jcx")

import json
path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format('2019-11-07')
alldata = sc.textFile(path)
all_vid = alldata.map(lambda x: (x.split('\t')[0], x.split('\t')[1])).filter(lambda x: 'profileinfo' in json.loads(x[1], encoding='utf-8')).filter(lambda x: 'cstatus' in json.loads(x[1], encoding='utf-8')).map(lambda x: "{}\t{}".format(x[0], json.loads(x[1], encoding='utf-8')['cstatus']))
all_vid.repartition(800).saveAsTextFile("hdfs://Ucluster/word_segment_result/profile")


my_sql = "select vid, status from dw.video"
df = spark.sql(my_sql)
vids = df.select(['vid','status']).rdd.map(lambda x: "{}\t{}".format(x.vid, x.status))
vids.repartition(800).saveAsTextFile("hdfs://Ucluster/word_segment_result/lib")


import sys
file1 = sys.argv[1]
file2 = sys.argv[2]
file3 = sys.argv[3]
with open(file1,'r') as f1, open(file2,'r') as f2, open(file3,'w') as w1:
    dict1 ={}
    dict2 = {}
    for line in f1:
        line = line.strip().split('\t')
        dict1[int(line[0])]=int(line[1])
    for line in f2:
        line =  line.strip().split('\t')
        dict2[int(line[0])]=int(line[1])

    res = set(dict2.keys()).difference(set(dict1.keys()))
    myres = list(res)
    print(len(myres))
    #for line in myres:
    #    myline = "{}\n".format(line)
    #    w1.write(myline)
    for vid in myres:
        if dict2[vid] == 0:
            myline = "{}\n".format(vid)
            w1.write(myline)




