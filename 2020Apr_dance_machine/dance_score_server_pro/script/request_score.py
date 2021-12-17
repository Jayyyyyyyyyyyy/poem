#coding:utf-8
import requests
import json
import base64

def getByte(path):
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str

def scores(jsonbody):
    url = "http://127.0.0.1:8903/polls/score"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

img = '../data/output_teacher/frame18_rendered.png'
str_encode = getByte(img)
parms = {"uid":123, "user_imgs": str_encode, 'teacher_frame_time': 20, 'flag': 2}  # img是ndarray，无法直接用base64编码，否则会报错
res = scores(parms)
res = json.loads(res)
print(res)


