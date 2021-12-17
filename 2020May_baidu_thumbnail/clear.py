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

    # headers = requests.utils.default_headers()
    # headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
    headers = {'content-type': 'application/x-www-form-urlencoded'}
    response = requests.post(request_url, data=params, headers=headers)
    return response





def get_urls(vid):
    key = 'url_{}'.format(vid)
    tmp = r.get(key)
    if tmp == None:
        return None
    res = json.loads(tmp)
    print(res)
    pic = 'http://aimg.tangdou.com'+res['pic']
    if '!' in pic:
        pic = pic.split('!')[0]
    return pic
cnt = 0
with open('./record.txt', 'r') as f, open('./record_batch2.txt' ,'w') as f2:
    for line in f:
        # obj = json.loads(line.strip())
        # vid = obj['id']
        # width = obj['profileinfo']['img_width']
        # if width<640:
        #     continue
        vid = line.strip()
        print(vid)
        url = get_urls(vid)
        if url:
            with urllib.request.urlopen(url) as url_file:
                try:
                    img = base64.b64encode(url_file.read())
                except:
                    print('read file fail {}'.format(vid))
                    continue
                response = enhance(img)
                if response:
                    res = response.json()
                    res['vid'] = vid
                    # res['url'] = url
                    line = json.dumps(res)
                    if 'image' in res:
                        img_decode = base64.b64decode(res['image'])  # base64解~A
                        img_np = np.frombuffer(img_decode, np.uint8)  # NbyteMO~V为np.array形O
                        img_tmp = cv2.imdecode(img_np, cv2.COLOR_RGB2BGR)
                        cv2.imwrite('./imgs2/{}.jpeg'.format(vid), img_tmp)
                        f2.write(line + '\n')
                    else:
                        print(res, vid)
                    cnt = cnt + 1
                    print(cnt)

