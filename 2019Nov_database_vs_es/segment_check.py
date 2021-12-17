# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/21 10:44 AM
# @File      : segment_check.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import requests
import json


def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    esvids = []
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['vid', 'title', 'uname', 'content_mp3', 'uid_team_name']}

        payload = json.dumps(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        reses = []
        json1 = response.json()['hits']['hits']
        for x in json1:
            title = ''
            uname = ''
            content_mp3 = ''
            teamname = ''

            res = x['_source']
            vid = res['vid']
            title = res['title']
            uname = res['uname']
            if 'content_mp3' in res:
                if res['content_mp3']:
                    content_mp3 = res['content_mp3']['tagname']
            teamname = res['uid_team_name']
            res = (vid, title, uname, content_mp3, teamname)
            reses.append(res)
        esvids.extend(reses)
    return esvids


vids = []
with open('no_seg', 'r') as r:
    for line in r:
        line = line.strip()
        vids.append(line)
results = eschecker(vids)
to_pickle = 'pickle_content'
pickle.dump(results, open(to_pickle, 'wb'), protocol=2)
with open('pickle_content', 'w') as file1:
    for line in results:
        new = {}
        new['vid'] = line[0]
        new['title'] = line[1]
        new['uname'] = line[2]
        new['content_mp3'] = line[3]
        new['teamname'] = line[4]
        res = json.dumps(new, ensure_ascii=False)
        file1.write(res + '\n')
