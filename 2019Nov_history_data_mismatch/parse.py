# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/14 11:37 AM
# @File      : parse.py
# @Software  : PyCharm
# @Company   : Xiao Tang


import json
import sys
import pandas as pd

def get_tagname_tagid(words, df_pair, df_tag_lib, content_name):
    tag = []
    for word in words:
        df_label = df_pair[df_pair.alias == word][['label']]
        # 产品要求status=1
        df_tag = df_tag_lib[(df_tag_lib.rec_field == content_name) & (df_tag_lib.name == word) & (df_tag_lib.status == 1)][['id']]
        tmp_df_tag = df_tag_lib[(df_tag_lib.rec_field == content_name) & (df_tag_lib.name == word) & (df_tag_lib.status == 0)][['pid']]
        if not df_label.empty:
            label_name = df_label.iat[0, 0]
            tag_id = df_tag_lib[(df_tag_lib.rec_field == content_name) & (df_tag_lib.name == label_name)][['id', 'name']]
            if not tag_id.empty:
                tag_id = tag_id.iat[0, 0]
                tag.append({'tagname': label_name, 'tagvalue': 1.0, 'tagid': int(tag_id)})
        elif not df_tag.empty:
            tag_id = df_tag.iat[0, 0]
            tag.append({'tagname': word, 'tagvalue': 1.0, 'tagid': int(tag_id)})
        elif not tmp_df_tag.empty:
            tag_pid = tmp_df_tag.iat[0, 0]
            new_tagname = df_tag_lib[df_tag_lib.id == tag_pid][['name']].iat[0, 0]
            tag.append({'tagname': new_tagname, 'tagvalue': 1.0, 'tagid': int(tag_pid)})
        else:
            continue
    if len(tag) == 0:
        return [{'tagname': 'unknown', 'tagvalue': 1.0, 'tagid': 0}]
    return tag



def post_process_content_dance_tag_mismatch_map(row):
    global df_tag_lib
    global dance_pair
    global tag_pair
    rec_dance_name = 'content_dance'
    rec_tag_name = 'content_tag'
    obj = json.loads(row)
    newdict = {}
    id = obj['id']
    newdict['id'] = id
    newdict['profileinfo'] = {}
    content_tag = obj['content_tag']
    content_dance = obj['content_dance']
    if content_tag:
        content_tag_names = list(filter((lambda x: x != 'unknown'),content_tag))
    else:
        content_tag_names = content_tag
    if content_dance:
        if content_dance == 'unknown':
            content_dance_name = content_dance.replace('unknown', '')
        else:
            content_dance_name = content_dance
    else:
        content_dance_name = content_dance

    if content_tag_names and not content_dance_name:
        new_content_dance = get_tagname_tagid(content_tag_names, dance_pair, df_tag_lib, rec_dance_name)[0]
        new_content_tag = get_tagname_tagid(content_tag_names, tag_pair, df_tag_lib, rec_tag_name)
    elif not content_tag_names and content_dance_name:
        new_content_dance = get_tagname_tagid([content_dance_name], dance_pair, df_tag_lib, rec_dance_name)[0]
        new_content_tag = get_tagname_tagid([content_dance_name], tag_pair, df_tag_lib, rec_tag_name)
    elif content_tag_names and content_dance_name:
        content_tag_names.append(content_dance_name)
        all_names = list(set(content_tag_names))
        all_names = list(filter((lambda x: x not in ['广场舞', '大众广场舞']),all_names))
        if content_tag_names:
            new_content_tag = get_tagname_tagid(all_names, tag_pair, df_tag_lib, rec_tag_name)
            new_content_dance = get_tagname_tagid([content_dance_name], dance_pair, df_tag_lib, rec_dance_name)[0]
        else:
            new_content_tag = get_tagname_tagid(['广场舞'.encode('utf8')], tag_pair, df_tag_lib, rec_tag_name)
            new_content_dance = get_tagname_tagid(['广场舞'.encode('utf8')], dance_pair, df_tag_lib, rec_dance_name)[0]
    else:
        newdict['profileinfo']['content_tag'] = content_tag
        newdict['profileinfo']['content_dance'] = content_dance
        return newdict
    newdict['profileinfo']['content_tag'] = new_content_tag
    newdict['profileinfo']['content_dance'] = new_content_dance
    return newdict



if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    history_file = sys.argv[1]
    output = sys.argv[2]
    tag_lib_path = './tag_lib.csv'
    dance_pair_path = './content_dance_pair.csv'
    tag_pair_path = './content_tag_pair.csv'

    df_tag_lib = pd.read_csv(tag_lib_path, delimiter=',', encoding='utf-8')
    tag_pair = pd.read_csv(tag_pair_path)
    dance_pair = pd.read_csv(dance_pair_path)
    a = 0
    with open(history_file, 'r', encoding='utf-8') as reader, open(output, 'w', encoding='utf-8') as writer:
        for line in reader:
            res = post_process_content_dance_tag_mismatch_map(line.strip())
            res = json.dumps(res,ensure_ascii=False)
            writer.write(res+'\n')
            a +=1




    # total = all_vid.count()
    # rule1 = rule_one.count()
    # rule2 = rule_two.count()
    # line1 = "content_dance in content_tag: {}/{}={}".format(rule1,total,round(rule1/total,4))
    # line2 = "content_dance in firstcat or secondcat: {}/{}={}".format(rule2,total,round(rule2/total,4))
    #
    # rule_one.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/content_dance_content_tag_dismatch")
    # rule_two.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/content_dance_cats_dismatch")
    #




