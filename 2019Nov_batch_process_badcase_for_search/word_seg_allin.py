#-*- coding: utf-8 -*-
'''
@Author  : CathyZhang
@Time    : 2019-02-28 18:20
@Software: PyCharm
@File    : pyspark_cut_key_word.py
'''
import sys
from pyspark.sql import SparkSession
import jieba
from HTMLParser import HTMLParser as html
import re
import numpy as np
import json
import requests
import pickle
reload(sys)
sys.setdefaultencoding('utf-8')



def parse(row):
    obj =  json.loads(row)
    vid = obj['vid']
    title = obj['title']
    uname = obj['uname']
    content_mp3 = obj['content_mp3']
    teamname = obj['teamname']
    return (vid, title, uname, content_mp3, teamname)

def parse_row(row):
    title = ""
    uname = ""
    content_mp3 = ""
    teamname = ""
    row = json.loads(row)
    vid = row['id']
    if 'videoinfo' in row:
        if 'title' in row['videoinfo']:
            title = row['videoinfo']['title'].strip()
    if 'profileinfo' in row:
        if 'uname' in row['profileinfo']:
            uname = row['profileinfo']['uname'].strip()
        if 'content_mp3' in row['profileinfo']:
            if 'tagname' in row['profileinfo']['content_mp3']:
                content_mp3 = row['profileinfo']['content_mp3']['tagname'].strip()
        if 'uid_team_name' in row['profileinfo']:
            teamname = row['profileinfo']['uid_team_name'].strip()
    return (vid, title, uname, content_mp3, teamname)


def eschecker(vids):
    vid_list = [vids[i:i + 5000] for i in range(0, len(vids), 5000)]
    res = []
    for v_list in vid_list:
        url = "http://10.19.87.8:9221/portrait_video_v1/video/_search"
        payload = {"size": 5000, "query": {"terms": {"id": v_list}},
                   "_source": ['vid', 'title', 'uname', 'content_mp3', 'uid_team_name']}

        payload = json.dumps(payload)
        headers = {
            'Content-Type': "application/json",
            'cache-control': "no-cache"
        }
        response = requests.request("POST", url, data=payload, headers=headers)
        json1 = response.json()['hits']['hits']
        for x in json1:
            title = ''
            uname = ''
            content_mp3 = ''
            teamname = ''

            res = x['_source']
            vid = res['vid']
            title = res['title']
            uname = res['uname']
            if 'content_mp3' in res:
                if res['content_mp3']:
                    content_mp3 = res['content_mp3']['tagname']
            teamname = res['uid_team_name']
            res = [vid, title, uname, content_mp3, teamname]

    return res



def _tokenize(content):
    if len(content.strip()) == 0:
        return []
    global jieba2
    """this dict will be loaded once in every partition"""
    if not jieba.dt.initialized:
        jieba.load_userdict(broadcast_user_dict.value)
    if jieba2 is None:
        jieba2 = jieba.Tokenizer()
    content = html().unescape(content)
    content = re.sub(ur"[^a-zA-Z0-9\u4e00-\u9fa5]+", " ", content)
    content = content.lower()
    re.sub('\s', ' ', content)
    content_full_tmp = jieba2.tokenize(content)
    result = jieba.tokenize(content, mode='search')
    result = list(result)
    for ele in content_full_tmp:
        if ele[0] not in [x[0] for x in result]:
            result.append(ele)
    result = sorted(result, key = lambda x: (x[1], x[2]))
    return result


def to_term_weight_dict(list):
    mydict = {}
    values = []
    for term, df, value in list:
        values.append(float(value))
        mydict[term] = float(value)
    avg = np.mean(values)
    median = np.median(values)
    return mydict, avg, median

def to_co_show_dict(data):
    co_show_dict = {}
    co_show_dict_sum = {}
    for line in data:
        line = line.strip()
        tokens = line.split("\t")
        if len(tokens)!=5:continue
        key = tokens[0]
        wordline = tokens[4]
        sum = 0
        if key not in co_show_dict:
            co_show_dict[key] = {}
        for wordstr in wordline.split(" "):
            subkey = wordstr.split(":")[0]
            value = float(wordstr.split(":")[-1])
            sum +=value
            co_show_dict[key][subkey] = value
        co_show_dict_sum[key] = sum
    return co_show_dict, co_show_dict_sum


def termweight_func(query, origin, isTitle):
    termlist = []
    valuelist = []
    term_score = []
    termweight = []
    tmp = {}
    if len(query) == 0:
        res = json.dumps(tmp)
        return res
    global co_show_dict
    global term_weight_dict
    wordlist3 = []
    for i, wordi in enumerate(query):
        if len(wordi) != 3: continue
        wordlist3.append(wordi)
    tmp_dict = {}

    for i, subtokeni in enumerate(wordlist3):
        wi = subtokeni[0]
        w_fromi = int(subtokeni[1])
        w_toi = int(subtokeni[2])
        for j, subtokenj in enumerate(wordlist3):
            if j > i:
                wj = subtokenj[0]
                w_fromj = int(subtokenj[1])
                w_toj = int(subtokenj[2])
                if w_fromi >= w_fromj and w_toi <= w_toj:
                    if wj in co_show_dict and wi in co_show_dict[wj]:
                        v2 = co_show_dict[wj][wi]
                        if wj not in tmp_dict:
                            tmp_dict[wj] = v2
                        else:
                            tmp_dict[wj] += v2
                        if wi not in tmp_dict:
                            tmp_dict[wi] = -v2
                        else:
                            tmp_dict[wi] += -v2
    # return '1'
    min =0
    max = 0
    for w, value in tmp_dict.items():
        if value < min:
            min = value
        if value > max:
            max = value
    jiange = max-min +0.0001
    sum2 = 0

    sum = 0
    for w in wordlist3:
        w = w[0]
        if w in term_weight_dict:
            termweight.append((w, term_weight_dict[w]))
            sum += term_weight_dict[w]
        else:
            termweight.append((w, 5.0))
            sum += 5.0
    # return termweight
    for (term, weight) in termweight:
        score = float(weight/sum)
        v2 = 0
        if term in tmp_dict:
            v2 = tmp_dict[term]
        score2 = score +score*float(v2)/jiange
        term_score.append((term,score2))
        sum2 +=score2
    # return term_score
    global stop_words_dict
    for (term, score) in term_score:
        if term not in stop_words_dict:
            if term == " ":
                term = "\w"
            value = round(float(score)/sum2, 4)
            termlist.append(term)
            valuelist.append(value)
    if isTitle == False:
        seg_set = set([x[0] for x in term_score])
        if origin in seg_set:
            pass
        else:
            termlist.append(origin)
            valuelist.append(1.0)

    tmp['term'] = termlist
    tmp['value'] = valuelist
    res = json.dumps(tmp, ensure_ascii=False)
    return res
def remove_space(row):
    return [ele.strip() for ele in row]


def to_stop_word_dict(data):
    words = []
    for line in data:
        words.append(line.strip())
    mydict = set(words)
    return mydict

def big_json(row):
    vid = row[0]
    title = json.loads(row[1])
    uname = json.loads(row[2])
    content_mp3 = json.loads(row[3])
    teamname = json.loads(row[4])
    tmp = {}
    tmp['vid'] = vid
    tmp['title'] = title
    tmp['uname'] = uname
    tmp['content_mp3'] = content_mp3
    tmp['teamname'] = teamname
    res = json.dumps(tmp, ensure_ascii=False)
    return res

if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    reload(sys)
    sys.setdefaultencoding('utf-8')

    #filein = sys.argv[1]
    jieba2 =None
    # initial spark
    spark = SparkSession.builder \
        .appName('title_keywords') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()


    user_dict_path = "hdfs://Ucluster/word_segment_jcx/dict/user_dict_v1017.txt"
    user_dict = spark.sparkContext.textFile(user_dict_path).collect()
    broadcast_user_dict = spark.sparkContext.broadcast(user_dict)

    print('debug204')
    term_weight_path = "hdfs://Ucluster/word_segment_jcx/dict/term_weight"
    term_weight_path_in_spark = spark.sparkContext.textFile(term_weight_path).collect()
    broadcast_term_weight = spark.sparkContext.broadcast(term_weight_path_in_spark)
    print('debug208')
    term_weight_list = [line.strip().split('\t') for line in broadcast_term_weight.value]
    term_weight_dict, avg, median = to_term_weight_dict(term_weight_list)
    print('debug211')
    co_show_path = 'hdfs://Ucluster/word_segment_jcx/dict/out_segmentv2.co_show'
    co_show_path_in_spark = spark.sparkContext.textFile(co_show_path).collect()
    co_show = spark.sparkContext.broadcast(co_show_path_in_spark)
    co_show_dict, co_show_dict_sum = to_co_show_dict(co_show.value)
    print('debug216')
    stop_word_path = 'hdfs://Ucluster/word_segment_jcx/dict/stop_words'
    stop_word_in_spark = spark.sparkContext.textFile(stop_word_path).collect()
    stop_words = spark.sparkContext.broadcast(stop_word_in_spark)
    stop_words_dict = to_stop_word_dict(stop_words.value)
    print('debug221')

    # online sql
    #data_path = "hdfs://Ucluster/word_segment_jcx/dict/{}".format('query_500w')
    #data_path = "hdfs://Ucluster/word_segment_result/no_seg"
    data_path = 'hdfs://Ucluster//word_segment_result/pickle_content'

    # my_rdd = pickle.load(open(data_path, 'rb'))
    # print(my_rdd[:10])
    # print(data_path)
    data_rdd = spark.sparkContext.textFile(data_path)
    # my_rdd = eschecker(data_rdd)
    # print(my_rdd[:10])
    #data_rdd = spark.sparkContext.parallelize(my_rdd)
    data_rdd = data_rdd.map(parse) #rdd = (vid, title, uname, content_mp3, teamname)
    # cut title to word
    data_rdd = data_rdd.filter(lambda x: len(x)==5)
    print(data_rdd.collect()[:10])
    data_rdd1 = data_rdd.filter(lambda x: len(x)!=5)
    print(data_rdd1.collect())
    print('debug227')
    cutted_df = data_rdd.map(lambda x: (x[0], _tokenize(x[1]), x[2], _tokenize(x[2]), x[3], _tokenize(x[3]), x[4], _tokenize(x[4]) )) #rdd = (vid, title, uname, seguname, content_mp3, segcontent_mp3, teamname, segteamname)
    print(cutted_df.collect()[:10])
    print('debug229')
    cutted_df = cutted_df.map(lambda x: (x[0],termweight_func(x[1],'', True), termweight_func(x[3],x[2], False) ,termweight_func(x[5], x[4], False) ,termweight_func(x[7],x[6], False) ))
    print(cutted_df.collect()[:10])
    #cutted_df = cutted_df.map(lambda x: (x[0], termweight_func(x[1],'', True), termweight_func(x[3],x[2], False) ,termweight_func(x[5], x[4], False) ,)   )
    #print(cutted_df.collect())
    cutted_df = cutted_df.map(big_json)
    print(cutted_df.collect()[:10])
    print('saving')
    cutted_df.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/jcx_rest_segment444")
    # hadoop fs -ls hdfs://Ucluster/word_segment_result2/out.txt
    # hadoop fs -rmr hdfs://Ucluster/word_segment_result2/out.txt
    # left_vid



# spark-submit --master yarn --deploy-mode cluster  --num-executors 60 --executor-memory 24g  --executor-cores 4  --driver-memory 24g --archives hdfs://Ucluster/word_segment_jcx/segment_jcx.zip#segment   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./segment/segment_jcx/bin/python   /home/hadoop/users/jcx/hive/spark_word_segment/word_seg_rest.py

# # # query_500w
# result_content_mp3_names.txt
# seg_title.csv
# seg_username.csv
# # # teamname.csv






