# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/21 10:44 AM
# @File      : status_check.py
# @Software  : PyCharm
# @Company   : Xiao Tang

"""
my_sql = "select vid from dw.video where status=0"
df = spark.sql(my_sql)
vid_rdd = df.rdd.map(lambda x: x.vid)
vid_rdd.saveAsTextFile('/word_segment_result/vid1126')
"""
import requests
import json


def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    esvids = []
    c = 0
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['cstatus','vid','title']}

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
            vid = res['vid']
            if 'cstatus' in res:
                cstatus = int(res['cstatus'])
                if cstatus == 0:
                    title = res['title']
                    if 'dj' in title.lower():
                        res = (vid, title)
                        reses.append(res)
            else:
                c +=1
        esvids.extend(reses)
    print(c)
    return esvids




def request_vtag(jsonbody):
    url = "http://10.19.37.142:9022/polls/vtag"
    line = {"vid": "1500671919067", "title": "一秋健身dj《小苹果》网红神曲动感健身操原创附分解", "uid": "", "uname": "一秋健身", "degree": "2",
            "teach": "2", "genre": "0", "child_category": "0", "content": "", "content_tag": "",
            "content_teach_id": "363", "content_degree_id": "353", "content_genre_id": "342", "content_raw_id": "358",
            "content_dance_id": "15", "createtime": "1573296754000", "user_tag": "", "video_effects": "-1",
            "video_tools": "-1", "video_mp3_name": "", "uid_team_name_seg": "", "ocr_text_seg": ""}
    # line = json.loads(line)

    nlp_vtagres = request_vtag(line)
    # print(nlp_vtagres)
    print(json.loads(nlp_vtagres))

    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

vids = []
# with open('vids_cstatus_0', 'r') as r:
#     for line in r:
#         line = line.strip()
#         vids.append(line)
with open('to_update_cstatus', 'r') as r:
    for line in r:
        line = line.strip()
        line = json.loads(line)['vid']
        vids.append(line)

results = eschecker(vids)


with open('to_update_cstatus2', 'w',encoding='utf-8') as file1:
    for line in results:
        new = {}
        new['vid'] = line[0]
        new['title'] = line[1]
        res = request_vtag(line[1])
        print(res)
        res = json.dumps(new, ensure_ascii=False)
        file1.write(res + '\n')
