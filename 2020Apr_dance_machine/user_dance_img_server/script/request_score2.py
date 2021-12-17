#coding:utf-8
import requests
import json
import base64
def getByte(path):
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str
def browse(jsonbody):
    url = "http://10.42.178.198:8905/polls/img_browse"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text
img = '/Users/tangdou1/Downloads/8_093045.jpg'
img = '/Users/tangdou1/PycharmProjects/poem/2020Apr_dance_machine/dance_score_server/data/teacher/frame43.jpg'
str_encode = getByte(img)
parms = {"uid":125, "time": 18, 'date': '2020-06-09', 'score': 80, 'pageid': 0, 'pagesize': 5}  # img是ndarray，无法直接用base64编码，否则会报错
res = browse(parms)
res = json.loads(res)
