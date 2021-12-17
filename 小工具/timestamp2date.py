# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/4 9:58 AM
# @File      : timestamp2date.py
# @Software  : PyCharm
# @Company   : Xiao Tang



from datetime import datetime
import time

# ts = 1323123124  # 单位秒
#
# datetime.fromtimestamp(ts).strftime('%Y-%m-%d')


"""
yesterday_object = datetime.strptime(yesterday, '%Y-%m-%d')
today_object = yesterday_object + timedelta(days=1)
today = today_object.strftime('%Y-%m-%d')
"""
print(time.ctime())

print( True and False)

# datetime to timestamp
createtime = '2019-08-02 14:49:14.0'
dateobj = datetime.strptime(createtime, '%Y-%m-%d %H:%M:%S.%f')
t = dateobj.timetuple()
timeStamp = int(time.mktime(t))
print(timeStamp)
dt_object = datetime.fromtimestamp(timeStamp)
print(type(dt_object))
print(dt_object.year)
d = datetime.date(dt_object)
print(d)
# unixtime = time.mktime(d.timetuple())
# print(dt_object)

