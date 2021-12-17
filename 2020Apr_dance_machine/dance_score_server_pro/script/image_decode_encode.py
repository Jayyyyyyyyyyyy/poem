# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/4/17 4:00 PM
# @File      : image_decode_encode.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import numpy as np
import cv2

def img_decode(str_encode):
    nparr = np.fromstring(str_encode, np.uint8)
    image_decode = cv2.imdecode(nparr,cv2.IMREAD_COLOR)
    return image_decode

def img_encode(img_path):
    img = cv2.imread(img_path)
    img_encode = cv2.imencode('.jpg', img)[1]
    data_encode = np.array(img_encode)
    str_encode = data_encode.tostring()
    return str_encode


img = '../data/user/frame4000.jpg'
str_encode = img_encode(img)
print(img_decode(str_encode))
print(cv2.imread(img))
