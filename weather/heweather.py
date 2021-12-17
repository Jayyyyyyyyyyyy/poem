# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/10/8 11:47 AM
# @File      : heweather.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import requests
import json
import pprint
pp = pprint.PrettyPrinter(indent=4)
city = '北京'
url_now = "https://free-api.heweather.net/s6/weather/now?location={}&key=0b7427ddec2a48fb8cdbb3b681395214".format(city)
url_forecast = "https://free-api.heweather.net/s6/weather/forecast?location={}&key=0b7427ddec2a48fb8cdbb3b681395214".format(city)
# url_hourly = "https://free-api.heweather.net/s6/weather/hourly?location={}&key=0b7427ddec2a48fb8cdbb3b681395214".format(city)
url_lifestyle = "https://free-api.heweather.net/s6/weather/lifestyle?location={}&key=0b7427ddec2a48fb8cdbb3b681395214".format(city)

r_now = requests.get(url_now).json()
r_forecast = requests.get(url_forecast).json()
# r_hourly = requests.get(url_hourly).json()
r_lifestyle = requests.get(url_lifestyle).json()

print(r_now)
now = r_now['HeWeather6'][0]['now']
forecast = r_forecast['HeWeather6'][0]['daily_forecast']
lifestyle = r_lifestyle['HeWeather6'][0]['lifestyle']
res = {}
res['now'] = now
res['forecast'] = forecast
res['lifestyle'] = lifestyle
final = responsejson = json.dumps(res, ensure_ascii=False)
#print(final)
# current weather
# pp.pprint(now)
# # three days forecast
# pp.pprint(forecast)
# # hourly forecast
# # pp.pprint(r_hourly)
# # lifestyle
# pp.pprint(lifestyle)


