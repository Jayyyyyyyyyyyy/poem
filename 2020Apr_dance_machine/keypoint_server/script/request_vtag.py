import requests
import json
from math import *

def keypoints(jsonbody):
    url = "http://127.0.0.1:9002/polls/querytag"
    url = "http://10.19.32.163:8902/polls/keypoints"
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

img = 'http://img0.pclady.com.cn/pclady/2003/22/1943663_745861.jpg'
parms = {"img": img}
image1 = json.loads(keypoints(parms))['keypoints'][0]
grs = {}
grs['Nose'] = image1[0]
grs['Neck'] = image1[1]
grs['RShoulder'] = image1[2]
grs['RElbow'] = image1[3]
grs['RWrist'] = image1[4]
grs['LShoulder'] = image1[5]
grs['LElbow'] = image1[6]
grs['LWrist'] = image1[7]
grs['MidHip'] = image1[8]
grs['RHip'] = image1[9]
grs['RKnee'] = image1[10]
grs['RAnkle'] = image1[11]
grs['LHip'] = image1[12]
grs['LKnee'] = image1[13]
grs['LAnkle'] = image1[14]


image2 = json.loads(keypoints(parms))['keypoints'][0]
cand = {}
cand['Nose'] = image2[0]
cand['Neck'] = image2[1]
cand['RShoulder'] = image2[2]
cand['RElbow'] = image2[3]
cand['RWrist'] = image2[4]
cand['LShoulder'] = image2[5]
cand['LElbow'] = image2[6]
cand['LWrist'] = image2[7]
cand['MidHip'] = image2[8]
cand['RHip'] = image2[9]
cand['RKnee'] = image2[10]
cand['RAnkle'] = image2[11]
cand['LHip'] = image2[12]
cand['LKnee'] = image2[13]
cand['LAnkle'] = image2[14]

def getAngleBetweenPoints(orig, landmark):
    x_orig, y_orig = orig[:2]
    x_landmark, y_landmark = landmark[:2]
    deltaY = y_landmark - y_orig
    deltaX = x_landmark - x_orig
    angleInDegrees = atan2(deltaY, deltaX) * 180 / pi
    return angleInDegrees + 180

angle_1 = getAngleBetweenPoints(grs['Neck'],grs['Nose'])
print(angle_1)
angle_2 = getAngleBetweenPoints(grs['Neck'],grs['RWrist'])
print(angle_2)
angle_3 = getAngleBetweenPoints(grs['Neck'],grs['LWrist'])
print(angle_3)
angle_4 = getAngleBetweenPoints(grs['Neck'],grs['RShoulder'])
print(angle_4)
angle_5 = getAngleBetweenPoints(grs['Neck'],grs['LShoulder'])
print(angle_5)
angle_6 = getAngleBetweenPoints(grs['Neck'],grs['RElbow'])
print(angle_6)
angle_7 = getAngleBetweenPoints(grs['Neck'],grs['LElbow'])
print(angle_7)
angle_8 = getAngleBetweenPoints(grs['Neck'],grs['MidHip'])
print(angle_8)
angle_9 = getAngleBetweenPoints(grs['Neck'],grs['RHip'])
print(angle_9)
angle_10 = getAngleBetweenPoints(grs['Neck'],grs['LHip'])
print(angle_10)