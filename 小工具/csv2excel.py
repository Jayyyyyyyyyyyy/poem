# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/4 10:24 AM
# @File      : csv2excel.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import pandas as pd
csv_file = '/Users/tangdou1/Documents/tag_lib.csv'
excel_file = '/Users/tangdou1/Documents/tag_lib.xlsx'
df = pd.read_csv(csv_file,encoding='utf-8')
df.to_excel(excel_file,encoding='utf-8',index=None)