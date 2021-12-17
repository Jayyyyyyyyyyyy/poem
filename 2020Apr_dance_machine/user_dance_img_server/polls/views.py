from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import logging
import json
import time
import pandas as pd
import base64
import math
import os


def DBC2SBC(ustring):
    #' 全角转半角 "
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if not (0x0021 <= inside_code and inside_code <= 0x7e):
            rstring += uchar
            continue
        rstring += chr(inside_code)
    return rstring

def getByte(path):
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str

def read_txt(path):
    with open(path, 'r') as f:
        tmp = []
        for line in f:
            line = line.replace('\n', '')
            tmp.append(line)
        string = "".join(tmp)
    return string


@csrf_exempt
def img_browse(request):
    if request.method == "POST":
        uid = request.POST.get('uid', '')
        teacher_time = request.POST.get('time', '')
        date = request.POST.get('date', '')
        score = request.POST.get('score', '')
        pageid = request.POST.get('pageid', '')
        pagesize = request.POST.get('pagesize', '')
    elif request.method == "GET":
        uid = request.GET.get('uid', '')
        teacher_time = request.GET.get('time', '')
        date = request.GET.get('date', '')
        score = request.GET.get('score', '')
        pageid = request.GET.get('pageid', '')
        pagesize = request.GET.get('pagesize', '')
    try:
        start = time.time()
        logging.info("Request:{}, {}, {}, {}, {}, {}".format(uid,teacher_time,date,score,pageid,pagesize))
        result = {}
        teacher_time = DBC2SBC(teacher_time)

        if len(teacher_time.strip()) == 0:
            responsejson = json.dumps({'code': 2, 'error': 'need teacher_time parameter'}, ensure_ascii=False)
            return HttpResponse(responsejson)
        if len(date) == 0:
            responsejson = json.dumps({'code': 2, 'error': 'need date parameter'}, ensure_ascii=False)
            return HttpResponse(responsejson)
        # if len(score) == 0:
        #     responsejson = json.dumps({'code': 2, 'error': 'need score parameter'}, ensure_ascii=False)
        #     return HttpResponse(responsejson)
        if len(pageid) == 0:
            responsejson = json.dumps({'code': 2, 'error': 'need pageid parameter'}, ensure_ascii=False)
            return HttpResponse(responsejson)
        if len(pagesize) == 0:
            responsejson = json.dumps({'code': 2, 'error': 'need pagesize parameter'}, ensure_ascii=False)
            return HttpResponse(responsejson)

        if ':' in teacher_time:
            min, sec = teacher_time.split(':')
            teacher_time = int(min)*60 + int(set)
        else:
            teacher_time = int(teacher_time.strip())



        # get zero ratio from csv file   @return field
        zero_rate_path = "./data/stats/stat_zero_rate_{}.csv".format(date)
        zero_rate_df = pd.read_csv(zero_rate_path)
        if teacher_time not in [int(x) for x in list(zero_rate_df['frame'])]:
            responsejson = json.dumps({'code': 2, 'error': 'time {} is not in time line'.format(teacher_time)}, ensure_ascii=False)
            return HttpResponse(responsejson)
        zero_rate = zero_rate_df[zero_rate_df['frame'] == teacher_time]['ratio'].values[0]
        result['zero_rate'] = round(zero_rate,4)*100

        # get teacher img string @ return filed
        #./data/teacher/frame20_rendered.png
        teacher_img = './data/teacher/frame{}.jpg'.format(teacher_time)
        teacher_img_string = getByte(teacher_img)
        result['teacher_img'] = teacher_img_string

        # get list of imgs @ return filed
        all_csv_path = "./data/stats/stat_{}.csv".format(date)
        all_df = pd.read_csv(all_csv_path)


        if len(score) != 0 and score in ['0', '80', '90', '100']:
            score = int(score)
            if len(uid) != 0:
                #filter_df = all_df[(all_df['frame'] == teacher_time) & (all_df['score'] == score) & (all_df['uid'] == uid)]
                filter_df = all_df[(all_df['frame'] == teacher_time) & (all_df['score'] == score)]
            else:
                filter_df = all_df[(all_df['frame'] == teacher_time) & (all_df['score'] == score)]
            # get total number of imgs @ return
            size_df = filter_df.shape[0]
            result['total_user_imgs'] = size_df

            pageid = int(pageid)
            pagesize = int(pagesize)
            imgs_array = []
            if pagesize <= 0:
                responsejson = json.dumps({'code': 2, 'error': 'pagesize has to greater than 0'}, ensure_ascii=False)
                return HttpResponse(responsejson)
            total_pages = math.floor(size_df/pagesize)
            if pageid > total_pages:
                responsejson = json.dumps({'code': 2, 'error':'pageid is out of total pages'}, ensure_ascii=False)
                return HttpResponse(responsejson)

            imgs_path = "./data/img_{}".format(date)
            filter_df = filter_df.reset_index()
            for index, row in filter_df.iterrows():
                if math.floor(index/pagesize) ==  pageid:
                    file_name = "{}_{}".format(row[1], row[3])
                    path = os.path.join(imgs_path, file_name)
                    img_detail = row[5]
                    img_str = read_txt(path)
                    tmp = {'img_string':img_str, 'img_score':score, 'img_detail': img_detail}
                    imgs_array.append(tmp)
            result['user_imgs_page'] = imgs_array
        else:
            if len(uid) != 0:
                # filter_df = all_df[(all_df['frame'] == teacher_time) & (all_df['score'] == score) & (all_df['uid'] == uid)]
                filter_df = all_df[(all_df['frame'] == teacher_time)]
            else:
                filter_df = all_df[(all_df['frame'] == teacher_time)]
            # get total number of imgs @ return
            size_df = filter_df.shape[0]
            result['total_user_imgs'] = size_df

            pageid = int(pageid)
            pagesize = int(pagesize)
            imgs_array = []
            if pagesize <= 0:
                responsejson = json.dumps({'code': 2, 'error': 'pagesize has to greater than 0'}, ensure_ascii=False)
                return HttpResponse(responsejson)
            total_pages = math.floor(size_df / pagesize)
            if pageid > total_pages:
                responsejson = json.dumps({'code': 2, 'error': 'pageid is out of total pages'}, ensure_ascii=False)
                return HttpResponse(responsejson)

            imgs_path = "./data/img_{}".format(date)
            filter_df = filter_df.reset_index()
            for index, row in filter_df.iterrows():
                if math.floor(index / pagesize) == pageid:
                    file_name = "{}_{}".format(row[1], row[3])
                    img_score = row[4]
                    path = os.path.join(imgs_path, file_name)
                    img_str = read_txt(path)
                    tmp = {'img_string':img_str, 'img_score':img_score}
                    imgs_array.append(tmp)
            result['user_imgs_page'] = imgs_array

        result['code'] = 1


        log_res = {}
        log_res['code'] = result['code']
        log_res['zero_rate'] = result['zero_rate']
        log_res['total_user_imgs'] = result['total_user_imgs']
        log_res['teacher_img'] = teacher_time
        log_res['score'] = score


        responsejson = json.dumps(result, ensure_ascii=False)
        my_log_res = json.dumps(log_res, ensure_ascii=False)

        end = time.time()
        logging.info("Query spent total time: " + str(end - start) + " seconds")
        logging.info("Response:" + my_log_res)
        return HttpResponse(responsejson)
    except Exception as e:
        logging.error("querytag exception: " + e)
        # result = {'code':2}
        # responsejson = json.dumps(result, ensure_ascii=False)
        # logging.info(responsejson+'305')
        # return HttpResponse(responsejson)







