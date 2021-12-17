import requests
import json
import base64

def getByte(path):
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str

def keypoints(jsonbody):
    url = "http://127.0.0.1:9002/polls/querytag"
    url = "http://10.19.6.41:8901/polls/keypoints"

    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

img = '/Users/tangdou1/Documents/user284.jpg'
img_str = getByte(img)
img_decode_ = img_str.encode('ascii')
parms = {"img": img_decode_}
print(keypoints(parms))
image1 = json.loads(keypoints(parms))['keypoints'][0]
print(image1)
