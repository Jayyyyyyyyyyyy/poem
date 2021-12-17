import json
with open('tmp_2021-06-15', 'r', encoding='utf-8') as f, open('res_2021-06-15','w') as f2:
    tmp = []
    for line in f:
        obj = json.loads(line.strip())
        img_width = obj['img_width']
        video_quality = obj['video_quality']
        totaldv = obj['totaldv']
        if totaldv == None:
            totaldv = 0
        vid = obj['vid']
        if img_width > 640 and video_quality< 1000:
            newline = "{}\t{}\t{}\n".format(vid, totaldv, video_quality)
            f2.write(newline)
