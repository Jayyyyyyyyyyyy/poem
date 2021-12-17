# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/4 10:26 AM
# @File      : my_pickle.py
# @Software  : PyCharm
# @Company   : Xiao Tang


import pandas as pd
import pickle
teacher_name_path = '/Users/tangdou1/PycharmProjects/poem/check_content_mp3_tag/mp3.txt'
#df = pd.read_csv(teacher_name_path)['label']
to_pickle = '/Users/tangdou1/PycharmProjects/poem/check_content_mp3_tag/mp3_name.pickle'
mylist = []
with open(teacher_name_path, 'r', encoding='utf-8') as reader:
    for line in reader:
        mylist.append(line[:-1])
df = pd.DataFrame(mylist, columns=['mp3name'])
pickle.dump(list(df['mp3name']), open(to_pickle,'wb'),protocol=2)


to_pickle = '/Users/tangdou1/Documents/mp3_name_dict.pickle'

vids = pickle.load(open(to_pickle,'rb'))
print(len(vids))
for i,x in enumerate(vids):
    print(x)
    if i == 100:
        break

