# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/2/14 5:14 PM
# @File      : crawling.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import csv
import requests
import json

fp = open("./res.json",'wt',newline='',encoding='utf-8')
qqfp = open("./qqmusic.csv", 'rt', newline='', encoding='utf-8')
reader = csv.reader(qqfp)
urls = []

for topid, rank_name in reader:
    myurl = "https://c.y.qq.com/v8/fcg-bin/fcg_v8_toplist_cp.fcg?topid={}&platform=yqq.json&jsonpCallback=MusicJsonCallbacktoplist".format(topid)
    urls.append([rank_name,myurl])


print(urls)
mydict = {}
for rank_name, url in urls:
    r = requests.get(url)
    results = json.loads(str(r.text[26:-1]))
    fins = results.get("songlist")
    mydict[rank_name]=[]
    for ind, fin in enumerate(fins):
        songnames = fin.get("data").get("songname")
        mydict[rank_name].append([ind, songnames])
        print(songnames)
res = json.dumps(mydict,ensure_ascii=False)
fp.write(res)
fp.close()
qqfp.close()