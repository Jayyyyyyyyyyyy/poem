# encoding:utf-8

import numpy as np
import cv2

import requests
import base64
import json
import urllib
import redis
import http.client
# 配置如下
# http.client.HTTPConnection._http_vsn = 10
# http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'
hostname = '10.42.158.47'
port = 6379
r = redis.Redis(host=hostname, port=port)

cnt = 0
with open('./record3.txt', 'r') as f:
    for line in f:
        obj = json.loads(line.strip())
        if 'image' not in obj:
            print(obj)
            continue
        img = obj['image']
        vid = obj['vid']
        img_decode = base64.b64decode(img)
        img_np = np.frombuffer(img_decode, np.uint8)  # NbyteMO~V为np.array形O
        img = cv2.imdecode(img_np, cv2.COLOR_RGB2BGR)
        cv2.imwrite('./imgs/{}.jpeg'.format(vid), img)


