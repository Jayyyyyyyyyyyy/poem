# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/6 10:48 AM
# @File      : compare_title_two.py
# @Software  : PyCharm
# @Company   : Xiao Tang
# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/29 3:33 PM
# @File      : refresh_content_mp3_seg.py
# @Software  : PyCharm
# @Company   : Xiao Tang


"""
my_sql = "select vid from dw.video where status=0 and length(title)<>0"
df = spark.sql(my_sql)
vid_rdd = df.rdd.map(lambda x: x.vid)
vid_rdd.saveAsTextFile('/word_segment_result/vid1215')
"""
import requests
import json
import sys
with open('./diff_title', 'r', encoding='utf-8') as r, open('./diff_title_update', 'w', encoding='utf-8') as w:
    for line in r:
        new = {}
        line = line.strip()
        if len(line.split('\t')) != 4:
            print(line)
            vid, status, db = line.split('\t')
            status = int(status)
            if status == 0:
                new['id'] = vid
                new['videoinfo'] = {}
                new['videoinfo']['title'] = db
                res = json.dumps(new, ensure_ascii=False)
                w.write(res + '\n')
        else:
            vid, status, db, es = line.split('\t')

            status = int(status)
            if status == 0:
                new['id'] = vid
                new['videoinfo'] = {}
                new['videoinfo']['title'] = db
                res = json.dumps(new, ensure_ascii=False)
                w.write(res + '\n')

