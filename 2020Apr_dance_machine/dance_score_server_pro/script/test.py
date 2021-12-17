import requests
import json
import cv2
import numpy as np
import base64

def img_encode(img_path):
    img = cv2.imread(img_path)
    img_encode = cv2.imencode('.jpg', img)[1]
    data_encode = np.array(img_encode)
    str_encode = data_encode.tostring()
    return str_encode

def getByte(path):
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str



def keypoints(jsonbody):
    url = "http://127.0.0.1:9002/polls/querytag"
    url = "http://10.19.32.163:8902/polls/keypoints"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

img = '/data/jiangcx/dance_score_server/data/user/frame4000.jpg'
img_str = getByte(img)
img_decode_ = img_str.encode('ascii')
parms = {"img": img_decode_}
image1 = json.loads(keypoints(parms))['keypoints'][0]
print(image1)

