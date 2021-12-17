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
    url = "http://10.10.25.182:8903/polls/score"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

img = '/Users/tangdou1/Downloads/8_093045.jpg'
img = '/Users/tangdou1/PycharmProjects/poem/2020Apr_dance_machine/dance_score_server/data/teacher/frame43.jpg'
str_encode = getByte(img)
#print(str_encode)
# for x in range(100):
parms = {"uid":124, "user_imgs": str_encode, 'teacher_frame_time': 79, 'flag': 2}  # img是ndarray，无法直接用base64编码，否则会报错
res = scores(parms)
res = json.loads(res)
print(res)

