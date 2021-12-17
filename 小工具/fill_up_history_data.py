
# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/8/31 6:24 PM
# @File      : lost.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import redis
from datetime import datetime
import pickle
import time


host = '10.19.127.152'
port = 6379
r = redis.Redis(host=host, port=port)
key = 'rec:video:new:list'
path = '/home/hadoop/users/jcx/hive/history_process/no_profile.pickle'
path = '/Users/tangdou1/Downloads/no_profile.pickle'
vids = pickle.load(open(path,'rb'))
vid_list = [vids[i:i+10000] for i in range(0,len(vids),10000)]

for ind, vids in enumerate(vid_list):
    print(ind)
    if 1500667889397 in vids:
        ind = vids.index(1500667889397)
        for vid in vids[ind+1:]:
            r.lpush(key, vid)
            time.sleep(0.5)
            print(vid)
    # if ind>7:
    #     for vid in vids:
    #         r.lpush(key, vid)
    #         time.sleep(0.5)
    #         print(vid)

time = datetime.today().strftime('%Y-%m-%d %H-%M-%S')
print('{} : Done!'.format(time))

