# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/2/18 10:47 AM
# @File      : analysis.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import csv
import re
import pandas as pd

def DBC2SBC( ustring):
    # ' 全角转半角 "
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if not (0x0021 <= inside_code and inside_code <= 0x7e):
            rstring += uchar
            continue
        rstring += chr(inside_code)
    return rstring

def remove_braces(name):
    name = DBC2SBC(name)
    return re.sub('\\(.*?\\)', '', name)

with open("./res.json", 'rt', newline='', encoding='utf-8') as f: #qqyinyue 排行榜 爬取结果
    res = f.readlines()
    qq_music_ranks = json.loads(res[0])

qqfp = open("./qqmusic.csv", 'rt', newline='', encoding='utf-8') #榜单名
reader = csv.reader(qqfp)
rank_names = []
rank_names = ["热歌榜","抖音排行榜","网络歌曲榜"]  # 选取的榜单名




with open('./out_0219', 'r', encoding='utf-8') as f: # query 结果['query',pv]
    mp3_tmp = []
    for line in f:
        mp3_obj = json.loads(line)
        mp3_tmp.append(mp3_obj)
df = pd.DataFrame(mp3_tmp)
newdf = df.groupby([0], as_index=False)[1].sum().reset_index()[[0,1]]

new_mp3_tmp = []
for index, (song, pv) in newdf.iterrows():
    new_mp3_tmp.append([song,pv])
top_querys = sorted(new_mp3_tmp, key=lambda x: x[1], reverse=True)


with open('./tmp_mp3', 'r', encoding='utf-8') as f2: # query 结果['query',pv]
    mp3_lib = []
    for line in f2:
        mp3_obj2 = json.loads(line)
        mp3_lib.append(mp3_obj2)
top_querys2 = sorted(mp3_lib,key = lambda x:x[1],reverse=True)[2:10000]
# queryfp = open("./query_top100.csv", 'rt', newline='', encoding='utf-8')
# reader2 = csv.reader(queryfp)
# top_querys = [x[0] for x in reader2]

def compare(song):
    global top_querys
    global top_querys2
    tmp1 = []
    tmp2 = []
    for query,pv in top_querys: #query
        if song == query:
            tmp1 = [query, pv]
            break

    for query, pv in top_querys2: #lib
        if song == query:
            tmp2 = [query,pv]
            break

    return tmp1, tmp2
for topid, rank_name in reader:
    rank_names.append(rank_name)
rank_names = ["热歌榜","抖音排行榜","网络歌曲榜"]  # 选取的榜单名

qqfp.close()
tmp_dict = {}
qqmusic_ranks = {}
for name in rank_names:
    rank_list = qq_music_ranks[name]
    tmp_dict[name]=[]
    qqmusic_songs = []
    for rank, song in rank_list:
        song = remove_braces(song).strip()
        search, lib = compare(song)

        if search != [] and lib != []:
            query, pv1 = search
            mp3, pv2 = lib
            line = [song, query, pv1, mp3, pv2]
        elif search != [] and lib == []:
            query, pv1 = search
            line = [song, query, pv1, '', -1]
        elif search == [] and lib != []:
            mp3, pv2 = lib
            line = [song, '', -1, mp3, pv2 ]
        else:
            line = [song, '', -1,  '', -1]

        qqmusic_songs.append(line)




    df = pd.DataFrame(qqmusic_songs,index=None, columns=['qq_music','search','pv','mp3_lib','num'])
    df.to_csv('{}.csv'.format(name),index=None)
    #qqmusic_ranks[name] = qqmusic_songs
    #diff = set([  remove_braces(x[1]).strip() for x in rank_list]).difference(set( [x[0][0][0] for x in tmp_dict[name] ]))
    #tname='{}_only'.format(name)
    #tmp_dict[tname]=list(diff)
    #tmp_dict[name] = [  x[1] for x in tmp_dict[name]]
# analysis = json.dumps(tmp_dict,ensure_ascii=False)
# with open('./result_query.json','w',encoding='utf-8') as myf:
#     myf.write(analysis)

