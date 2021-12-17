# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/10/23 6:14 PM
# @File      : local_test.py
# @Software  : PyCharm
# @Company   : Xiao Tang

# ./conda create -n segment_jcx   --copy  -y -q python=2.7.14
# ./conda/source activate segment_jcx
# conda install --copy -c conda-forge jieba

import json
import jieba
import html
import re
import sys
import numpy as np


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
    for line in open(data,'r'):
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


# clearning query  ==>> Task 0.
def cleaning_query(raw_query):
    clean_query = html.unescape(raw_query.strip())
    clean_query = re.sub("[^a-zA-Z0-9\u4e00-\u9fa5]+", "", clean_query)
    clean_query = clean_query.lower()
    re.sub('\s', '', clean_query)
    return clean_query


# words segment  ==>> Task 1.
def words_segment(query, pku_cut=True, jieba_cut=True):
    text = {}
    if jieba_cut:
        jieba_seg_res1 = jieba.cut(query)
        # jieba_seg_res1 = " ".join(jieba_seg_res1)
        jieba_seg_res = jieba.cut_for_search(query)
        # jieba_seg_res = " ".join(jieba_seg_res)
        result = jieba.tokenize(query, mode='search')
        position = " ".join(["{}:{}:{}".format(tk[0], tk[1], tk[2]) for tk in result])
        text['jieba_jingzhun'] = jieba_seg_res1
        text['jieba'] = jieba_seg_res
        text['posi_query'] = position

    return text


# jcx


def termweight_func(query):

    ## 加入共现字典的termweight  计算 搜索模式

    wordlist3 = []
    for i, wordi in enumerate(query):
        subtokeni = wordi.split(":")
        if len(subtokeni) != 3: continue
        wordlist3.append(subtokeni)

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
    min =0
    max = 0
    for w, value in tmp_dict.items():
        if value < min:
            min = value
        if value > max:
            max = value
    jiange = max-min +0.0001

    term_score = []
    sum2 = 0
    termweight = []
    sum = 0
    for w in wordlist3:
        w = w[0]
        if w in term_weight_dict:
            termweight.append((w, term_weight_dict[w]))
            sum += term_weight_dict[w]
        else:
            termweight.append(w, 10.0)
            sum += 10
    for (term, weight) in termweight:
        score = float(weight/sum)
        v2 = 0
        if term in tmp_dict:
            v2 = tmp_dict[term]
        score2 = score +score*float(v2)/jiange
        term_score.append((term,score2))
        sum2 +=score2
        #outline +=str(term) +"/"+ str(round(score,4))+" "


    #归一化
    outline2 = ""
    for (term, score) in term_score:
        score2 = float(score)/sum2
        outline2 += str(term) + "/" + str(round(score2, 4)) + " "
    return outline2





def process_query(title):

    # add  childcategory_id = "0"
    if len(title.strip()) == 0:
        return '', '', ''
    title = cleaning_query(title)
    segments = words_segment(title)
    cutted_title = list(segments['jieba_jingzhun'])
    sum_normal = sum( [ term_weight_dict[term]  if term in term_weight_dict else  avg  for term in cutted_title ] )
    weighted_cutted_title =  [ "{}/{}".format(term, term_weight_dict[term]/sum_normal ) if term in term_weight_dict else "{}/{}".format(term, avg/sum_normal ) for term in cutted_title]

    cutted_title_seach = list(segments['jieba'])
    sum_search = sum( [ term_weight_dict[term]  if term in term_weight_dict else  avg  for term in  cutted_title_seach] )
    weighted_cutted_title_search = [ "{}/{}".format(term, term_weight_dict[term]/sum_search ) if term in term_weight_dict else "{}/{}".format(term, avg/sum_search) for term in cutted_title_seach]

    query_with_posi = segments['posi_query'].split(' ')
    segment_better = termweight_func(query_with_posi)

    segment_def = " ".join(weighted_cutted_title)
    segment_seach = " ".join(weighted_cutted_title_search)

    return segment_def, segment_seach, segment_better


def querytag(querytitle):
    print(len(querytitle))
    segment_def, segment_seach, segment_better = process_query(querytitle)
    result = {}
    result['query'] = querytitle
    result['segment_def'] = segment_def
    result['segment_seach'] = segment_seach
    result['segment_better'] = segment_better
    responsejson = json.dumps({'tag': result}, ensure_ascii=False)
    return responsejson

if __name__ == '__main__':
    file_in = sys.argv[1]
    file_out = sys.argv[2]
    user_dict_path = './dict/user_dict_v1017.txt'
    jieba.initialize()
    jieba.load_userdict(user_dict_path)
    # jieba load end
    print("************Success loading jieba")

    term_weight_path = "./dict/term_weight"
    term_weight_list = [line.strip().split('\t') for line in open(term_weight_path,'r')]
    term_weight_dict, avg, median = to_term_weight_dict(term_weight_list)
    print("************Success loading term_weight")

    co_show_path = './dict/out_segmentv2.co_show'
    co_show_dict, co_show_dict_sum = to_co_show_dict(co_show_path)
    print("************Success loading co_show_dict ,co_show_dict_sum")
    with open(file_in,'r',encoding='utf-8') as reader, open(file_out, 'w', encoding='utf-8') as writer:
        for line in reader:
            res = querytag(line)
            new_line = "{}\n".format(res)
            writer.write(new_line)
    