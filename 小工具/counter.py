# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/6 4:48 PM
# @File      : counter.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import pickle
from collections import Counter
import pandas as pd
to_pickle = '/Users/tangdou1/Documents/mp3_name_dict.pickle'

names = pickle.load(open(to_pickle,'rb'))
res = Counter(names)
abc = []
for x in res.items():
    abc.append(x)

df = pd.DataFrame(abc,columns=['mp3_name','cnt'])
#df.to_excel('/Users/tangdou1/Downloads/mp3_name.xlsx',encoding='utf-8',index=None)
df.to_csv('/Users/tangdou1/Downloads/mp3_name.csv',encoding='utf-8',index=None)
print(set(df['mp3_name']))
