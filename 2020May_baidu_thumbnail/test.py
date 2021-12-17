# encoding:utf-8

# import numpy as np
# import cv2

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
'''
图像清晰度增强
'''

request_url = "https://aip.baidubce.com/rest/2.0/image-process/v1/image_definition_enhance"
# 二进制方式打开图片文件

'''
https://aip.baidubce.com/oauth/2.0/token?grant_type=client_credentials&client_id=Dmf4b4ikfxenZyqgAIKvzxzS&client_secret=1EWPURTNo78DazSKYvuz4p2pDKGDC56g

'''
def enhance(img):
    request_url = "https://aip.baidubce.com/rest/2.0/image-process/v1/image_definition_enhance"
    params = {"image":img}
    access_token = "24.ea11a0b6f746115d55a79a3f95b6152d.2592000.1626404078.282335-23922126"
    request_url = request_url + "?access_token=" + access_token
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    response = requests.post(request_url, data=params, headers=headers)
    return response





def get_urls(vid):
    key = 'url_{}'.format(vid)
    tmp = r.get(key)
    if tmp == None:
        return None
    res = json.loads(tmp)
    pic = 'http://aimg.tangdou.com'+res['pic']
    if '!' in pic:
        pic = pic.split('!')[0]
    return pic
cnt = 0

vid = 1500679201553
url = get_urls(vid)
if url:
    with urllib.request.urlopen(url) as url_file:
        try:
            img = base64.b64encode(url_file.read())
        except:
            print('read file fail {}'.format(vid))
        response = enhance(img)
        if response:
            res = response.json()
            res['vid'] = vid
            res['url'] = url
            line = json.dumps(res)
            # print(res.keys())
            # img_decode = base64.b64decode(res)  # base64解~A
            # img_np = np.frombuffer(img_decode, np.uint8)  # NbyteMO~V为np.array形O
            # img = cv2.imdecode(img_np, cv2.COLOR_RGB2BGR)
            # cv2.imwrite('./enhance/{}.jpeg'.format(vid), img)
            cnt = cnt + 1
            print(cnt)
            print(vid)
