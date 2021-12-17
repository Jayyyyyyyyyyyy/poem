# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/11/12 11:45 AM
# @File      : spark_filter.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import json
import sys
from pyspark.sql import SparkSession
def parse_line(row):
    jsonobj = json.loads(row, encoding='utf-8')
    id = jsonobj['id']
    if 'content_tag' not in jsonobj['profileinfo']:
        content_tag = []
    else:
        if jsonobj['profileinfo']['content_tag'] == []:
            content_tag = []
        else:
            content_tag = [x['tagname'] for x in jsonobj['profileinfo']['content_tag'] if 'tagname' in x]
    if 'content_dance' not in jsonobj['profileinfo']:
        content_dance = ''
    else:
        if 'tagname' not in jsonobj['profileinfo']['content_dance']:
            content_dance = ''
        else:
            content_dance = jsonobj['profileinfo']['content_dance']['tagname']
    if 'firstcat' not in jsonobj['profileinfo']:
        firstcat = ''
    else:
        if 'tagname' not in jsonobj['profileinfo']['firstcat']:
            firstcat = ''
        else:
            firstcat = jsonobj['profileinfo']['firstcat']['tagname']
    if 'secondcat' not in jsonobj['profileinfo']:
        secondcat = ''
    else:
        if 'tagname' not in jsonobj['profileinfo']['secondcat']:
            secondcat = ''
        else:
            secondcat = jsonobj['profileinfo']['secondcat']['tagname']
    newdict = {}
    newdict['id'] = id
    newdict['content_tag'] = content_tag
    newdict['content_dance'] = content_dance
    newdict['firstcat'] = firstcat
    newdict['secondcat'] = secondcat
    newrow = json.dumps(newdict, ensure_ascii=False)
    return newrow

def get_tagname_tagid(words, df_pair, df_tag_lib, content_name):
    tag = []
    for word in words:
        df_label = df_pair.filter(df_pair['alias'] == word)
        df_tag = df_tag_lib.filter(df_tag_lib['rec_field'] == content_name).filter(df_tag_lib['name'] == word).filter(df_tag_lib['status'] == 1)
        if not df_label.rdd.isEmpty():
            label_name = df_label.collect()[0][0]
            tag_id = df_tag_lib.filter([(df_tag_lib['rec_field'] == content_name) & (df_tag_lib['name'] == label_name)])['id', 'name']
            if not tag_id.rdd.isEmpty():
                tag_id = tag_id.collect()[0][0]
                tag.append({'tagname': label_name, 'tagvalue': 1.0, 'tagid': int(tag_id)})
        elif not df_tag.rdd.isEmpty():
            tag_id = df_tag.collect()[0][0]
            tag.append({'tagname': word, 'tagvalue': 1.0, 'tagid': int(tag_id)})
        else:
            continue
    if len(tag) == 0:
        return [{'tagname': 'unknown', 'tagvalue': 1.0, 'tagid': 0}]
    return tag



def post_process_content_dance_tag_mismatch(row):
    obj = json.loads(row)
    content_tag = obj['content_tag']
    content_dance = obj['content_dance']
    if content_tag:
        content_tag_names = filter((lambda x: x != 'unknown'),content_tag)
    else:
        content_tag_names =content_tag
    if content_dance:
        if content_dance == 'unknown':
            content_dance_name = content_dance.replace('unknown', '')
        else:
            content_dance_name = content_dance
    else:
        content_dance_name = content_dance
    if content_tag_names and not content_dance_name:
        return True
    elif not content_tag_names and content_dance_name:
        return True
    elif content_tag_names and content_dance_name:
        return True
    else:
        return False


def post_process_content_dance_tag_mismatch_map(row):
    global df_tag_lib
    global dance_pair
    global tag_pair
    rec_dance_name = 'content_dance'
    rec_tag_name = 'content_tag'
    obj = json.loads(row)
    newdict = {}
    id = obj['id']
    content_tag = obj['content_tag']
    content_dance = obj['content_dance']
    if content_tag:
        content_tag_names = filter((lambda x: x != 'unknown'),content_tag)
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
        new_content_tag = content_tag
    elif not content_tag_names and content_dance_name:
        new_content_dance = content_dance
        new_content_tag = get_tagname_tagid(list(content_dance_name), tag_pair, df_tag_lib, rec_tag_name)
    elif content_tag_names and content_dance_name:
        content_tag_names.append(content_dance_name)
        all_names = list(set(content_tag_names))
        all_names = filter((lambda x: x not in ['广场舞', '大众广场舞']),all_names)
        if content_tag_names:
            new_content_tag = get_tagname_tagid(all_names, tag_pair, df_tag_lib, rec_tag_name)
            new_content_dance = get_tagname_tagid([content_dance_name], dance_pair, df_tag_lib, rec_dance_name)[0]
        else:
            new_content_tag = get_tagname_tagid(['广场舞'.encode('utf8')], tag_pair, df_tag_lib, rec_tag_name)
            new_content_dance = get_tagname_tagid(['广场舞'.encode('utf8')], dance_pair, df_tag_lib, rec_dance_name)[0]
    else:
        newdict['id'] = id
        newdict['content_tag'] = content_tag
        newdict['content_dance'] = content_dance
        return newdict
    newdict['id'] = id
    newdict['content_tag'] = new_content_tag
    newdict['content_dance'] = new_content_dance
    return newdict



if __name__ == '__main__':
    print("let begin")
    print(sys.argv)
    date = sys.argv[1]
    spark = SparkSession.builder \
        .appName('title_keywords') \
        .config('spark.sql.warehouse.dir', '/user/hive/warehouse') \
        .enableHiveSupport().getOrCreate()




    tag_lib_path = 'hdfs://Ucluster/word_segment_jcx/dict/tag_lib.csv'
    dance_pair_path = 'hdfs://Ucluster/word_segment_jcx/dict/content_dance_pair.csv'
    tag_pair_path = 'hdfs://Ucluster/word_segment_jcx/dict/content_tag_pair.csv'
    df_tag_lib = spark.read.csv(tag_lib_path, header=True)
    dance_pair = spark.read.csv(dance_pair_path, header=True)
    tag_pair = spark.read.csv(tag_pair_path, header=True)
    tag_pair.show()
    # tag_lib_in_spark = spark.sparkContext.textFile(tag_lib_path).collect()
    # df_tag = spark.sparkContext.broadcast(tag_lib_in_spark)
    # df_tag = [ x.split(',') for x in df_tag.value]
    # df_tag_lib = pd.DataFrame(data=df_tag[1:],columns=df_tag[0])

    # dance_pair_in_spark = spark.sparkContext.textFile(dance_pair_path).collect()
    # df_dance = spark.sparkContext.broadcast(dance_pair_in_spark)
    # df_dance = [x.split(',') for x in df_dance.value]
    # dance_pair = pd.DataFrame(data=df_dance[1:], columns=df_dance[0])
    # print(dance_pair)
    # print(dance_pair['alias'])
    # tag_pair_in_spark = spark.sparkContext.textFile(tag_pair_path).collect()
    # df_tag_pair = spark.sparkContext.broadcast(tag_pair_in_spark)
    # df_tag_pair = [x.split(',') for x in df_tag_pair.value]
    # tag_pair = pd.DataFrame(data=df_tag_pair[1:], columns=df_tag_pair[0])
    # print(tag_pair)
    # print(tag_pair['alias'])

    path = "hdfs://10.42.4.78:8020/olap/da/video_clean/{}/*".format('2019-11-12')
    alldata = spark.sparkContext.textFile(path)
    all_vid = alldata.map(lambda x: x.split('\t')[1]).filter(lambda x: 'profileinfo' in json.loads(x, encoding='utf-8')).filter(lambda x:'cstage' in json.loads(x, encoding='utf-8')).filter(lambda x: json.loads(x, encoding='utf-8')['cstage']!=8).map(parse_line)
    rule_one = all_vid.filter(post_process_content_dance_tag_mismatch)
    rule_one.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/mismatch_for_update")
    # rule_one = rule_one.map(post_process_content_dance_tag_mismatch_map)
    # print(rule_one.take(10))




    # total = all_vid.count()
    # rule1 = rule_one.count()
    # rule2 = rule_two.count()
    # line1 = "content_dance in content_tag: {}/{}={}".format(rule1,total,round(rule1/total,4))
    # line2 = "content_dance in firstcat or secondcat: {}/{}={}".format(rule2,total,round(rule2/total,4))
    #
    # rule_one.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/content_dance_content_tag_dismatch")
    # rule_two.repartition(1).saveAsTextFile("hdfs://Ucluster/word_segment_result/content_dance_cats_dismatch")
    #



