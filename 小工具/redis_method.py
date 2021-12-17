import redis
import json
r = redis.Redis(host='10.42.158.47', port=6379)
def func(vid):
    print(vid)
    key = "url_{}".format(vid)
    abc = r.get(key)
    pic = 'http://aimg.tangdou.com' + json.loads(abc)['pic']
    print(pic)
    video = 'http://aqiniudl.tangdou.com/' + json.loads(abc)['videourl']+'-10.mp4'
    print(video)
    return pic
#func(1500672245423)

# !/usr/bin/env python
# -*- coding: UTF-8 -*-
# 作用：统计某个前缀key的个数，并将其输入到文件
# 使用方法：python scan_redis.py apus* 100

import sys
import redis
import os

pool = redis.ConnectionPool(host='10.42.158.47', port=6379, db=0)
r = redis.Redis(connection_pool=pool)
# 扫描匹配值，通过sys.argv传参
match = "qtag_*"
# 每次匹配数量
count = 1000
# print match
# print count
# 总数量
total = 0
# 扫描到的key输出到文件
path = os.getcwd()
# 扫描到的key输出的文件
txt = path + ".keys.txt"
f = open(txt, "w")
for key in r.scan_iter(match=match, count=count):
    #   f.write("%s %s" % (key,"\n"))
    f.write(key + "\n")
    total = total + 1
f.close

print("匹配: %s 的数量为:%d " % (match, total))