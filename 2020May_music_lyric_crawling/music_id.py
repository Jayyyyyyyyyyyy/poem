# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/10/8 7:43 PM
# @File      : search.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import requests
import execjs
import json
import re
import pandas as pd
import time
import random
import ast
from lyric_by_music import LyricComment
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


def wangyi_search(song_id):
    header={
        'Cache-Control':'max-age=0',
        'Cookie':'_ntes_nuid=16a42b02260b7b091899fdfe44ee36d5; __f_=1517818018835; vjuids=-384d52583.16168b64799.0.90747366b31e8; vjlast=1517880232.1517880232.30; _ntes_nnid=16a42b02260b7b091899fdfe44ee36d5,1517880231840; vinfo_n_f_l_n3=d71018b33b6778e8.1.0.1517880231854.0.1517880258969; JSESSIONID-WYYY=uOVEcIJi3PyQpmZq4tYHNt8NfH9FOj4cCUson4NE4%2F%2FiqMJxNlclpuPNceaFBRU%2F5%2FmM4JriGz3l9Cg1BEwxsrENOZjQKFAVHh%2BwxEK%2FC2uJ782JKeI9EFCG8xEbp3Y4Rf3J4DwJEQMHiN5nKAjxKQeo1TihQxXcJH3dUQyx%5CG8AGa6i%3A1518769837399; _iuqxldmzr_=32; __utma=94650624.744896615.1518768038.1518768038.1518768038.1; __utmb=94650624.4.10.1518768038; __utmc=94650624; __utmz=94650624.1518768038.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic',
        'Host':'music.163.com',
        'Origin':'http://music.163.com',
        'Referer':'http://music.163.com/search/',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.104 Safari/537.36 Core/1.53.4785.400 QQBrowser/9.7.12991.400',
    }

    url='http://music.163.com/weapi/cloudsearch/get/web?csrf_token='

    with open('core1.js','r') as f:
        js_code=f.read()
    #song_id='一技梅'

    p2='010001'
    p3='00e0b509f6259df8642dbc35662901477df22677ec152b5ff68ace615bb7b725152b3ab17a876aea8a5aa76d2e417629ec4ee341f56135fccf695280104e0312ecbda92557c93870114af6c9d05c4f7f0c3685b7a46bee255932575cce10b424d813cfe4875d3e82047b97ddef52741d546b8e289dc6935b3ece0462db0a22b8e7'
    p4='0CoJUm6Qyw8W8jud'

    p=execjs.compile(js_code).call('d', '{"hlpretag":"","hlposttag":"","s":"'+song_id+'","type":"1","offset":"0","total":"true","limit":"30","csrf_token":""}',p2,p3,p4)
    # print(p['encText'])
    # print(p['encSecKey'])
    data={
        'params':p['encText'],
        'encSecKey':p['encSecKey'],
    }
    response=requests.post(url,data=data,headers=header)

    # 打印获取到数据
    content = response.text
    content=json.loads(content)

    if 'result' not in content:
        res = ['','']
        return res

    if 'songs' not in content['result']:
        res = ['','']
    else:
        song_list=content['result']['songs']
        res = [[remove_braces(x['name']).lower().strip(),x['id']] for x in song_list][:2]
        tmp_res = [x[0] for x in res]
        if song_id in tmp_res:
            res = res[tmp_res.index(song_id)]
        else:
            res = res[0]
    return res



# def qq_music_search(song_name):
#     p, n, w = 1, 5, song_name
#     url_1 = 'https://c.y.qq.com/soso/fcgi-bin/client_search_cp?aggr=1&cr=1&flag_qc=0&p={}&n={}&w={}'.format(p, n, w)
#     response1 = requests.get(url_1)
#     # 获取返回参数并且删除多余空格
#     text = response1.text.strip()
#     # 删除多余的字符使之符合json格式进行转换
#     j_datas = json.loads(text[9: len(text) - 1])
#     if len(j_datas['data']['song']['list'][0]['grp']) == 0:
#         tmpres = ['','']
#     else:
#         new_songname = j_datas['data']['song']['list'][0]['grp'][0]['songname']
#         songid = j_datas['data']['song']['list'][0]['grp'][0]['songid']
#         songmid = j_datas['data']['song']['list'][0]['grp'][0]['songmid']
#         tmpres = [new_songname, songid]
#     return tmpres
#

my_lyric = LyricComment()

tmp = []
with open('./mp3','r',encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        line = ast.literal_eval(line)
        if line[1]<=5:
            continue
        tmp.append(line)
desc_song_list = sorted(tmp, key=lambda x:x[1],reverse=True)
is_song = []
not_song= []
with open('/Users/tangdou1/PycharmProjects/poem/2020Mar_tangdou_lyric/find.txt', 'a', encoding='utf-8') as w1: #, open('/Users/tangdou1/PycharmProjects/poem/2020Mar_tangdou_lyric/not_find.txt', 'w', encoding='utf-8' ) as w2:

    for line, total in desc_song_list:
        songname = line.strip()
        wangyi = wangyi_search(songname)
        # wangyi = qq_music_search(songname)
        # sl = random.randint(1,5)
        # time.sleep(sl)
        wangyi_res = [songname]+ wangyi+[total] ## 'song_name','music_name','music_id','number'
        if wangyi_res[0] == wangyi_res[1]: # 如果被搜索歌曲名与搜索的歌曲名一样，则爬取歌词
            lyric = my_lyric.saveLyric(wangyi_res[2])
            print(lyric)
            w1.write(json.dumps(wangyi_res,ensure_ascii=False)+'\n')
            is_song.append(wangyi_res)

        else:
            pass
        newdf = pd.DataFrame(is_song, columns=['song_name', 'music_name', 'music_id', 'number'])
        music_id_path = './find.csv'
        newdf.to_csv(music_id_path, index=None)
