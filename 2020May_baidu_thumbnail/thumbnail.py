#!/usr/bin/env python
#coding=utf-8

#导入Python标准日志模块
import logging

#从Python SDK导入Media配置管理模块以及安全认证模块
from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials

#设置MediaClient的Host，Access Key ID和Secret Access Key
media_host = "http://media.bj.baidubce.com"
access_key_id = "44e3a2f6389d4e0ca7d18b020fabf887"
secret_access_key = "9a44984baabe4542abe66920bd9f7ed0"

#设置日志文件的句柄和日志级别
logger = logging.getLogger('baidubce.services.media.mediaclient')
fh = logging.FileHandler("sample.log")
fh.setLevel(logging.DEBUG)

#设置日志文件输出的顺序、结构和内容
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.setLevel(logging.DEBUG)
logger.addHandler(fh)

#创建BceClientConfiguration
config = BceClientConfiguration(credentials=BceCredentials(access_key_id, secret_access_key), endpoint = media_host)






from baidubce.bce_client_configuration import BceClientConfiguration
from baidubce.auth.bce_credentials import BceCredentials
from baidubce.protocol import HTTP
from baidubce.region import BEIJING
from baidubce.services.media.media_client import MediaClient

media_host = "http://media.bj.baidubce.com"
access_key_id = "44e3a2f6389d4e0ca7d18b020fabf887"
secret_access_key = "9a44984baabe4542abe66920bd9f7ed0"

my_config = BceClientConfiguration(
        credentials = BceCredentials(access_key_id, secret_access_key),
        endpoint = media_host,
        protocol = HTTP,
        region = BEIJING,
        connection_timeout_in_mills = 50 * 1000,
        send_buf_size = 1024 * 1024,
        recv_buf_size = 10 * 1024 * 1024)


# create MediaClient with my config
client = MediaClient(my_config)

pipelines = client.list_pipelines()
for pipeline in pipelines.pipelines:
  print pipeline


pipeline_name = 'simple_test'
source = {'key': 'dance.mp4'}
target = {"keyPrefix": "idl_extract",}
capture = {"mode": "idl"}
response = client.create_thumbnail_job(pipeline_name,  source, target, capture)
print response
