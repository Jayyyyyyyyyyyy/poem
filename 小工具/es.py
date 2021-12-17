# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/4 10:28 AM
# @File      : es.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import pandas as pd
import urllib
import json
from datetime import datetime
path = '/Users/tangdou1/Documents/多品类需要确定是否在标签体系.xlsx'
df = pd.read_excel(path,header = None)
vidlist = df[0].to_list()
normal, error = 0, 0
cstage = []
utime = []
no_profile = []
path1 = 'no_video_profile'
path2 = 'has_video_profile'
with open(path1, 'w') as write1, open(path2, 'w') as write2:
    for vid in vidlist:
        newobj = {}
        url = "http://10.10.101.226:9221/portrait_video/video/{}".format(vid)
        req = urllib.request.Request(url=url)
        try:
            res = urllib.request.urlopen(req)
            res = res.read()
            jsonobj = json.loads(res,encoding='utf-8')
            utime = jsonobj['_source']['utime']
            if len(utime) == 10:
                utime = int(utime)
            else:
                utime = int(utime)/1000
            utime = datetime.utcfromtimestamp(utime).strftime('%Y-%m-%d')
            cstage = jsonobj['_source']['cstage']
            cstatus = jsonobj['_source']['cstatus']
            newobj['vid'] = vid
            newobj['utime'] = utime
            newobj['cstage'] = cstage
            newobj['cstatus'] = cstatus
            line = json.dumps(newobj, ensure_ascii=False)
            write2.write(line + '\n')
            normal +=1
#             if normal >= 50:
#                 break
        except:
            write1.write(str(vid)+'\n')
            no_profile.append(vid)
            error += 1

print(normal,error)



import requests
import pickle

path = '/Users/tangdou1/Documents/lost_history_vids.pickle'
vids = pickle.load(open(path,'rb'))
print(len(vids))
vid_list = [vids[i:i+10000] for i in range(0,len(vids),10000)]
for ind, vids in enumerate(vid_list):
    if 1500667495334 in vids:
        print(ind)
ccc=[]
with open('/Users/tangdou1/Downloads/dpl_yes.txt','w',encoding='utf-8') as dpl_yes, open('/Users/tangdou1/Downloads/dpl_no.txt','w',encoding='utf-8') as dpl_no:
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"query": {"terms": {"id": v_list}},"_source": ""}
        payload = json.dumps(payload)
        headers = {
    'Content-Type': "application/json",
    'cache-control': "no-cache"
}
        response = requests.request("POST", url, data=payload, headers=headers)
        yes = [int(x['_id']) for x in response.json()['hits']['hits']]
        ccc.extend(yes)