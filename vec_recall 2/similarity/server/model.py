# -*- coding: utf-8 -*-
from termcolor import colored
from annoy import AnnoyIndex
import pickle
import os
import time
import datetime
import numpy as np
import subprocess
import json
from sklearn import preprocessing
from .helper import set_logger

__all__ = ['Config', 'AnnoyModel']


class Config:
    DIM = 1024
    TREE = 100
    METRIC = 'euclidean'
    INDEX_NAME = 'annoy.ann'
    ID2VID_NAME = 'id2vid.pkl'
    SOURCE_PATH = 'hdfs://10.42.31.63:8020/user/jiangcx/dup_img_feature/img_feature_all_'
    UPDATE_PATH = 'similarity/data/update_data/img_feature_all_'


class AnnoyModel:
    def __init__(self, ):
        self.__logger = set_logger(colored('MODEL', 'green'), verbose=True)
        self.__dim = Config.DIM
        self.__metric = Config.METRIC
        self.model = {}
        path_list = os.listdir('similarity/data/')
        path_list = list(filter(lambda x: Config.ID2VID_NAME in x or Config.INDEX_NAME in x, path_list))
        try:
            if not path_list and os.listdir('similarity/data/update_data'):
                tic = time.time()
                suffix = datetime.datetime.now().strftime("%Y-%m-%d")
                ts = datetime.datetime.now().strftime("%H%M")
                id2vid, embeds = self._parse_data_file(Config.UPDATE_PATH + suffix)
                self.model['index'] = self.build(embeds)
                self.model['id2vid'] = id2vid
                self.__id2vid_path = 'similarity/data/' + Config.ID2VID_NAME + '.' + suffix + '.ts-' + ts
                self.__index_path = 'similarity/data/' + Config.INDEX_NAME + '.' + suffix + '.ts-' + ts

                self.model['index'].save(self.__index_path)
                with open(self.__id2vid_path, 'wb') as f:
                    pickle.dump(id2vid, f)
                # with open('similarity/data/' + Config.EMBED_NAME + '.' + suffix + '.ts-' + ts, 'wb') as f:
                #     pickle.dump(embeds, f)
                toc = time.time()
                self.__logger.info('initial from source file cost time:{}'.format(toc - tic))
            else:
                self.load_model()
                self.__logger.info('initial from cache file succeed!')
        except Exception as e:
            self.__logger.error('initial error: ' + str(e))
            os.system('kill ' + str(os.getpid()))

    def __init_path(self, ):
        path_ist = os.listdir('similarity/data/')
        path_ist = list(filter(lambda x: not x.startswith('.'), path_ist))
        self.__logger.info("data path lists:{}".format(path_ist))
        index_path = None
        id2vid_path = None
        # embed_path = None
        file_list = list(filter(lambda x: os.path.isfile(os.path.join('similarity/data/', x)), path_ist))
        self.__logger.info('data path file lists:{}'.format(file_list))
        if len(file_list) != 2:
            self.__logger.error('data 下面文件数目有问题，请确保id2vid, annoy前缀的文件都只有一个')
            # return index_path, id2vid_path, embed_path
            return index_path, id2vid_path
        for name in path_ist:
            if Config.INDEX_NAME in name:
                index_path = 'similarity/data/' + name
            elif Config.ID2VID_NAME in name:
                id2vid_path = 'similarity/data/' + name
            # elif Config.EMBED_NAME in name:
            #     embed_path = 'similarity/data/' + name
        # return index_path, id2vid_path, embed_path
        return index_path, id2vid_path

    def load_model(self):
        self.__index_path, self.__id2vid_path = self.__init_path()
        assert all([self.__index_path, self.__id2vid_path]), \
            "数据文件不够，请检查data文件里是否含有annoy.ann，id2vid.pkl 前置的两个（有后缀，则两个文件后缀日期都应该相同）文件"

        self.model['index'] = AnnoyIndex(self.__dim, self.__metric)
        self.model['index'].load(self.__index_path)
        self.__logger.info('successfully load Annoy index, id is:%s'%(id(self.model['index'])))

        with open(self.__id2vid_path, 'rb') as f:
            self.model['id2vid'] = pickle.load(f)
        self.__logger.info('successfully load Annoy id2vid, id is:%s'%(id(self.model['id2vid'])))
        self.__logger.info('id2vid sample is :\n {}'.format(dict(list(self.model['id2vid'].items())[:10])))

        # return self.model

    def search_by_vector(self, vid, embed, batch_mode=0, recall_k=10, include_distances=False):
        if batch_mode == 0:
            # embeds = np.array([-np.float64(i.replace('-', '')) if i.startswith('-') else np.float64(i) for i in embed.strip().split(',')])
            embeds = preprocessing.normalize(np.array(embed).reshape(1, self.__dim), norm='l2')
            tmp_ret = self.model['index'].get_nns_by_vector(embeds.reshape(self.__dim), recall_k, include_distances=include_distances)
            tmp_dict = {}
            tmp_dict['vid'] = vid
            if include_distances:
                I = tmp_ret[0]
                D = tmp_ret[1]
                distance = [str(np.float32((2 - d ** 2) / 2)) for d in D]
                id2v = [self.model['id2vid'][i] for i in I]
                tmp = []
                for i in range(len(distance)):
                    tmp.append((id2v[i], np.float(distance[i])))
                tmp_dict['recall'] = tmp
            else:
                tmp_dict['recall'] = [self.model['id2vid'][i] for i in tmp_ret]
            return tmp_dict
        else:
            # embeds = []
            # for e in embed:
            #     ret = np.array([-np.float64(i.replace('-', '')) if i.startswith('-') else np.float64(i) for i in
            #                     e.strip().split(',')])
            #     embeds.append(ret)
            embeds = np.array(embed)
            embeds = preprocessing.normalize(embeds, norm='l2')
            batch_ret = []
            for i in range(len(vid)):
                tmp_ret = self.model['index'].get_nns_by_vector(embeds[i], recall_k, include_distances=include_distances)
                tmp_dict = {}
                tmp_dict['vid'] = vid[i]
                if include_distances:
                    I = tmp_ret[0]
                    D = tmp_ret[1]
                    distance = [str(np.float32((2-d**2)/2)) for d in D]
                    id2v = [self.model['id2vid'][i] for i in I]
                    tmp = []
                    for i in range(len(distance)):
                        tmp.append((id2v[i], np.float(distance[i])))
                    tmp_dict['recall'] = tmp
                else:
                    tmp_dict['recall'] = [self.model['id2vid'][i] for i in tmp_ret]
                batch_ret.append(tmp_dict)
            return batch_ret
    
    def build(self, embeds):
        self.__logger.info('start building annoy index')
        embed = preprocessing.normalize(embeds, norm='l2')
        index = AnnoyIndex(self.__dim, self.__metric)
        for k, v in enumerate(embed):
            index.add_item(k, v)
        index.build(Config.TREE)
        self.__logger.info('annoy index build success')
        return index

    def _parse_data_file(self, input_path):
        embed_ret = []
        vids = []
        with open(input_path, 'r', encoding='utf-8') as fi:
            for line in fi:
                if line.strip() == '':
                    continue
                try:
                    line = json.loads(line)
                    embed = line['feature']
                    if not embed or len(embed) != Config.DIM:
                        continue
                except:
                    self.__logger.info('cannot parse this line') 
                # embed = [-np.float64(i.replace('-', '')) if i.startswith('-') else np.float64(i) for i in embed]
                embed_ret.append(embed)
                vids.append(str(line['vid']))

        self.__logger.info('_parse_data_file: total num is:{}'.format(len(vids)))
        id2vid = {i: val for i, val in enumerate(vids)}
        return id2vid, np.array(embed_ret).astype(np.float64)

    def get_update_data(self):
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        # tmp_path = Config.UPDATE_PATH + today + '_tmp'
        # today_path = Config.UPDATE_PATH + today
        cmd = 'hdfs dfs -getmerge ' + Config.SOURCE_PATH + today + ' ' + Config.UPDATE_PATH + today
        self.__logger.debug('cmd: %s' % cmd)
        cmd_ret = subprocess.getstatusoutput(cmd)

        if cmd_ret[0] != 0:
            self.__logger.error('获取hdfs文件出错:{}'.format(cmd_ret[1]))
            return False

        # mv_cmd = 'mv ' + tmp_path + ' ' + today_path
        # self.__logger.debug('mv_cmd: %s' % mv_cmd)
        # mv_cmd_ret = subprocess.getstatusoutput(mv_cmd)
        # if mv_cmd_ret[0] != 0:
        #     self.__logger.error('临时文件移动出错:{}'.format(mv_cmd_ret[1]))
        #     return False

        # 删除昨天的全量数据
        del_cmd = 'rm ' + Config.UPDATE_PATH + yesterday
        self.__logger.debug('del_cmd:%s' % del_cmd)
        del_cmd_ret = subprocess.getstatusoutput(del_cmd)
        if del_cmd_ret[0] != 0:
            self.__logger.warning('删除昨天文件出错:{}'.format(del_cmd_ret[1]))
        return True

    def update_all(self, ts):
        self.__logger.info('start updating new_data……')
        suffix = datetime.datetime.now().strftime("%Y-%m-%d")
        id2vid, embeds = self._parse_data_file(Config.UPDATE_PATH + suffix)
        index = self.build(embeds)

        self.__logger.info('start saving new_data and updating new file path')
        new_index_path = 'similarity/data/' + Config.INDEX_NAME + '.' + suffix + '.ts-' + ts
        new_id2vid_path = 'similarity/data/' + Config.ID2VID_NAME + '.' + suffix + '.ts-' + ts

        # 持久化新的index和id2vid
        try:
            index.save(new_index_path)
        except Exception as e:
            os.remove(new_index_path)
            self.__logger.error('update index file faild! %s' % str(e))
            raise Exception('update index file faild!')

        try:
            with open(new_id2vid_path, 'wb') as f:
                pickle.dump(id2vid, f)
        except Exception as e:
            # 确保同一天的这两个文件是同时存在的
            os.remove(new_index_path)
            # os.remove(new_embed_path)
            os.remove(new_id2vid_path)
            self.__logger.error('update id2vid file faild! %s' % str(e))
            raise Exception('update id2vid file faild!')

        # 更新模型内存数据
        self.model['id2vid'] = id2vid
        self.model['index'] = index

        self.__logger.info("updated Annoy model's id2vid sample is :\n {}".format(dict(list(self.model['id2vid'].items())[:10])))

        # 删除来删除旧的文件路径， 并保持新的内容
        os.remove(self.__index_path)
        os.remove(self.__id2vid_path)
        self.__logger.debug('remove files : ' + self.__index_path + ', ' + self.__id2vid_path)

        self.__index_path = new_index_path
        self.__id2vid_path = new_id2vid_path
        self.__logger.debug('new paths are: ' + self.__index_path + ', ' + self.__id2vid_path)

        self.__logger.info("successfully update all data and files!")
