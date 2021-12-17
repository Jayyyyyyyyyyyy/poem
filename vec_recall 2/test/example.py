import requests
import uuid
import json
import numpy as np
import time

def request(datafile, recall_k=3, mode=0, batch=2, include_distance=1):
    vids, embeds = [], []
    with open(datafile, 'r', encoding='utf-8') as f:
        for line in f:
            line = json.loads(line)
            vids.append(str(line['vid']))
            embeds.append(line['feature'])
    if mode == 0:
        for i in range(len(vids)):
            vid = vids[i]
            embed = embeds[i]
            start = time.time()
             # 请求参数vid, embed必须有; include_distance可以不设置，默认为1
            r = requests.post(
                "http://127.0.0.1:5012/recall",
                json={
                        "vid": vid, 
                        "embed": embed,
                        "batch_mode": mode,
                        "include_distance": include_distance,
                }
            )
            end = time.time()
            print(end-start)
            print(r.text)
            # break
    else:
        # 请求参数vid, embed, batch_mode必须有, 并且batch_mode为1,include_distance可以不设置，默认为0
        r = requests.post(
            "http://127.0.0.1:5012/recall",
            json={
                    "vid": [vids[1], vids[2], vids[3]],
                    "embed": [embeds[1], embeds[2], embeds[3]],
                    "batch_mode": mode,
                    "include_distance": include_distance,
            }
        )
        print(r.text)





if __name__ == "__main__":
    request('sample2')
    # request('sample', mode=1)
    #request('sample2', mode=1, include_distance=0)

