#!/usr/bin/python
#coding:utf-8
'''
@author：Byron
新浪微博：http:#weibo.com/ziyuetk
'''
import urllib.request, json
from city import city

#yourcity = raw_input("你想查那个城市的天气？")

yourcity = '徐州'
#测试yourcity变量是否可以返回你想要的值
#print yourcity

url = "http:#www.weather.com.cn/data/cityinfo/" + city[yourcity] + ".html"

#print url #同上

response = urllib.request.urlopen(url, timeout=10)
city_dict = response.read()

#此处打印出来是一个json格式的东西
print (city_dict)

#用Python的json库来解析获取到的网页json内容
jsondata = json.loads(city_dict)


try:
    city = jsondata["city"]
    #日期
    date_y = jsondata["date_y"]
    #星期几
    week = jsondata["week"]
    #摄氏温度
    temp1 = jsondata["temp1"]
    temp2 = jsondata["temp2"]
    temp3 = jsondata["temp3"]
    temp4 = jsondata["temp4"]
    temp5 = jsondata["temp5"]
    temp6 = jsondata["temp6"]
    # 华氏温度
    tempF1 = jsondata["tempF1"]
    tempF2 = jsondata["tempF2"]
    tempF3 = jsondata["tempF3"]
    tempF4 = jsondata["tempF4"]
    tempF5 = jsondata["tempF5"]
    tempF6 = jsondata["tempF6"]
    # 天气描述
    weather1 = jsondata["weather1"]
    weather2 = jsondata["weather2"]
    weather3 = jsondata["weather3"]
    weather4 = jsondata["weather4"]
    weather5 = jsondata["weather5"]
    weather6 = jsondata["weather6"]
    # 天气描述对应图片
    img1 = jsondata["img1"]
    img2 = jsondata["img2"]
    img3 = jsondata["img3"]
    img4 = jsondata["img4"]
    img5 = jsondata["img5"]
    img6 = jsondata["img6"]
    img7 = jsondata["img7"]
    img8 = jsondata["img8"]
    img9 = jsondata["img9"]
    img10 = jsondata["img10"]
    img11 = jsondata["img11"]
    img12 = jsondata["img12"]
    img_single = jsondata["img_single"]
    # 图片名称
    img_title1 = jsondata["img_title1"]
    img_title2 = jsondata["img_title2"]
    img_title3 = jsondata["img_title3"]
    img_title4 = jsondata["img_title4"]
    img_title5 = jsondata["img_title5"]
    img_title6 = jsondata["img_title6"]
    img_title7 = jsondata["img_title7"]
    img_title8 = jsondata["img_title8"]
    img_title9 = jsondata["img_title9"]
    img_title10 = jsondata["img_title10"]
    img_title11 = jsondata["img_title11"]
    img_title12 = jsondata["img_title12"]
    img_title_single = jsondata["img_title_single"]
    # 风俗描述
    wind1 = jsondata["wind1"]
    wind2 = jsondata["wind2"]
    wind3 = jsondata["wind3"]
    wind4 = jsondata["wind4"]
    wind5 = jsondata["wind5"]
    wind6 = jsondata["wind6"]
    # 风速级别描述
    fx1 = jsondata["fx1"]
    fx2 = jsondata["fx2"]
    fl1 = jsondata["fl1"]
    fl2 = jsondata["fl2"]
    fl3 = jsondata["fl3"]
    fl4 = jsondata["fl4"]
    fl5 = jsondata["fl5"]
    fl6 = jsondata["fl6"]
    # 今天穿衣指数
    index = jsondata["index"]
    index_d = jsondata["index_d"]
    # 48小时穿衣指数
    index48 = jsondata["index48"]
    index48_d = jsondata["index48_d"]
    # 紫外线及48小时紫外线
    index_uv = jsondata["index_uv"]
    index48_uv = jsondata["index48_uv"]
    # 洗车
    index_xc = jsondata["index_xc"]
    # 旅游
    index_tr = jsondata["index_tr"]
    # 舒适指数
    index_co = jsondata["index_co"]
    # 晨练
    index_cl = jsondata["index_cl"]
    # 晾晒
    index_ls = jsondata["index_ls"]
    # 过敏
    index_ag = jsondata["index_ag"]

    #定义几个变量用来储存解析出来的内容
    temp_low = jsondata['weatherinfo']['temp1']
    temp_high = jsondata['weatherinfo']['temp2']
    weather = jsondata['weatherinfo']['weather']
except:
    print('no')
print (yourcity)
print (weather)
print (temp_low + "~" + temp_high)