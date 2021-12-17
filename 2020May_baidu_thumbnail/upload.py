

import os
import re
import redis
hostname = '10.10.42.123'
port = 6379
r = redis.Redis(host=hostname, port=port)

hostname2 = '10.19.5.157'
port = 6379
r2 = redis.Redis(host=hostname2, port=port)

cnt = 0
# with open('/Users/jiangcx/Downloads/img_enhance_vids','r') as f:
#     for line in f:
#         cnt += 1
#         print(cnt)
#         line = line.strip()
#         key = 'img_enhance_{}'.format(line)
#         if r2.exists(key):
#             continue
#         value = r.get(key).decode()
#         r2.set(key, value)
import time
g = os.walk("./enhance")
for path,dir_list,file_list in g:
    for file_name in file_list:
        if 'jpeg' not in file_name:
            continue
        img = os.path.join(path, file_name)
        print(img)
        vid = re.sub("\D", "", file_name)
        # file_name = '/data/jiangcx/img_enhance/test.jpeg'
        upload_file_name = '/public/feed/2021/0616/{}.jpg'.format(vid)
        curl = ' curl -i -F "file=@{}" -F "savepath={}" "http://10.10.81.62:88/uploader/store?_from=图片清晰度增强批次2&_host=10-42-178-198&ip=10.42.178.198" '.format(img, upload_file_name)
        key = 'img_enhance_{}'.format(vid)
        print(cnt)
        if cnt%100 == 0:
            time.sleep(60)
        r.set(key, upload_file_name)
        print(key)
        print(upload_file_name)
        os.popen(curl)
        cnt += 1

        # if cnt > 2:
        #     break