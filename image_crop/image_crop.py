# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/10 10:09 AM
# @File      : image_crop.py
# @Software  : PyCharm
# @Company   : Xiao Tang

from PIL import Image
import numpy as np

img = Image.open('/Users/tangdou1/PycharmProjects/poem/image_crop/3941575939771_.pic_hd.png')
img = img.convert("RGBA")
datas = img.getdata()
print(np.array(datas))
newData = []
for item in datas:
    if item[0] == 255 and item[1] == 255 and item[2] == 255:
        newData.append((255, 255, 255, 0))
    else:
        if item[0] > 150:
            newData.append((0, 0, 0, 255))
        else:
            newData.append(item)
            print(item)


img.putdata(newData)
img.save("img2.png", "PNG")
# if __name__ == '__main__':
#     img=Image.open('/Users/tangdou1/PycharmProjects/poem/image_crop/3941575939771_.pic_hd.jpg')
#     img=transparent_back(img)
#     img.save('img2.png')
