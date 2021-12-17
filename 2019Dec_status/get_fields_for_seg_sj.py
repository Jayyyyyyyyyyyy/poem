# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/23 9:09 PM
# @File      : get_fields_for_seg_sj.py
# @Software  : PyCharm
# @Company   : Xiao Tang
# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/12/16 8:50 PM
# @File      : extract_from_es.py
# @Software  : PyCharm
# @Company   : Xiao Tang

"""
my_sql = "select vid from dw.video where status<>0 and length(title)<>0"
df = spark.sql(my_sql)
vid_rdd = df.rdd.map(lambda x: x.vid)
vid_rdd.saveAsTextFile('/word_segment_result/vid1216')
"""
import requests
import json
import sys
import pickle

file1 = sys.argv[1]
file2 = sys.argv[2]

def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    esvids = []
    c = 0
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['vid', 'title', 'uname', 'uid_team_name', 'content_mp3', 'ocr_text']}

        payload = json.dumps(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        reses = []
        json1 = response.json()['hits']['hits']
        for x in json1:
            my_dict = {}
            res = x['_source']
            if 'vid' in res:
                vid = res['vid']
            else:
                continue
            try:
                title = res['title']
                content_mp3 = res['content_mp3']
                uname = res['uname']
                uid_team_name = res['uid_team_name']
                ocr_text = res['ocr_text']
                #res = (vid, title_seg,content_mp3_seg)
                my_dict['vid']= vid
                my_dict['title'] = title
                my_dict['content_mp3'] = content_mp3
                my_dict['uname'] = uname
                my_dict['uid_team_name'] = uid_team_name
                my_dict['ocr_text'] = ocr_text
                newline = json.dumps(my_dict,ensure_ascii=False)
                reses.append(newline)
            except:
                print(vid)
            # if 'title' in res:
            #      title = res['title']
            #     if 'dj' in title.lower():
            #         res = (vid, title,content_mp3)
            #         reses.append(res)
            # else:
            #     c +=1
        esvids.extend(reses)
    return esvids

vids = []
c = 0
with open(file1, 'r') as r:
    for line in r:
        line = line.strip()
        vids.append(line)
        c += 1


results = eschecker(vids)
# pickle.dump(results, open('./tmp_pickle','wb'),protocol=2)


#results = pickle.load(open('./tmp_pickle','rb'))
with open(file2, 'w',encoding='utf-8') as f:
    for x in results:
        f.write(x+'\n')

#
#
# import requests
# import json
# import sys
# import pickle
# file1 = sys.argv[1]
# file2 = sys.argv[2]
# with open(file1, 'r',encoding='utf-8') as r,  open(file2, 'w',encoding='utf-8') as w:
#     count_mp3_name_diff = 0
#     c = 0
#     for line in r:
#         try:
#             line = line.strip()
#             jsonobj = json.loads(line)
#             c += 1
#             new = {}
#             vid = jsonobj['vid']
#             title_seg = jsonobj['title_seg']
#             content_mp3_seg = jsonobj['content_mp3_seg']
#
#             tmp_segment = [x['name'] for x in title_seg]
#             segment = ["{}/{}".format(x['name'],x['weight']) for x in title_seg]
#             segment = " ".join(segment)
#             tmp_content_mp3_seg = [x['name'] for x in content_mp3_seg]
#             diff = list(set(tmp_content_mp3_seg).difference(set(tmp_segment)))
#             if len(diff) != 0:
#                 for x in diff:
#                     segment = "{} {}/1.0".format(segment, x)
#                 new['id'] =  vid
#                 new['profileinfo']={}
#                 new['profileinfo']['segment'] = segment
#                 res = json.dumps(new, ensure_ascii=False)
#                 w.write(res +'\n')
#                 # if c%10000==0:
#         except:
#             print(line)
#         # else:
#         #     new['id'] = line[0]
#         #     new['profileinfo'] = {}
#         #     new['profileinfo']['content_mp3'] = new_content_mp3
#         #     new['profileinfo']['content_mp3_seg'] = jsonboj['tag']['content_mp3_seg']
#         #     res = json.dumps(new, ensure_ascii=False)
#         #     file1.write(res + "\001" + content_mp3['tagname'] + '\n')
#         #
#         #     new['profileinfo']['content_mp3_seg'] = jsonboj['tag']['content_mp3_seg']
#         #
#         #     res = json.dumps(new, ensure_ascii=False)
#         #     print(res)
#         #     file1.write(res+"\001"+content_mp3['tagname']+'\n')



