# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/2/18 8:00 PM
# @File      : parse_tmp_mp3.py
# @Software  : PyCharm
# @Company   : Xiao Tang


import requests
import json
def wx_search(jsonbody):
    url = "http://10.42.16.15:9002/polls/querytag"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

with open('./query_7days_0220', 'r', encoding='utf-8') as f, open('./out_top','w',encoding='utf-8') as wf:
    tmp = []
    cnt = 0
    for line in f:
        cnt += 1
        print(cnt)
        query, pv = json.loads(line)
        parms = {"title": query}

        try:
            result = wx_search(parms)
            tmpres = json.loads(result)
            if 'correct' in tmpres['tag']:
                content_mp3 = tmpres['tag']['correct']['content_mp3']
                if 'tagname' in content_mp3:
                    content_mp3 = content_mp3['tagname']
                else:
                    continue
            else:
                content_mp3 = tmpres['tag']['original']['content_mp3']
                if 'tagname' in content_mp3:
                    content_mp3 = content_mp3['tagname']
                else:
                    continue
        except:
            continue
        r = json.dumps([content_mp3,pv],ensure_ascii=False)
        wf.write(r+'\n')




# print(cnt)
#pd.DataFrame(rows,columns=['query','pku','jieba']).to_excel('top50000.xlsx', index=None)