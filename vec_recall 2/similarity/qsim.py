# -*-coding: utf-8 -*-
import json
import datetime
from flask import Flask, Response, request
from flask_json import FlaskJSON, JsonError
from termcolor import colored

from .server.model import AnnoyModel
from .server.helper import set_timeFile_logger

from flask_apscheduler import APScheduler
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.WARNING)

app = Flask(__name__)
FlaskJSON(app)  #使用flask_json前需要初始化
logger = set_timeFile_logger(colored('ROUTE', 'red'))

model = AnnoyModel()

# scheduler 注册到 app
scheduler = APScheduler()  # 实例化APScheduler
scheduler.api_enabled = True
scheduler.init_app(app)  # 把任务列表放进flask
scheduler.start()  # 启动任务列表


@scheduler.task(trigger='cron', id='update_data', day_of_week='0-6', hour=2, minute=30)
# @scheduler.task(trigger='interval', hours=1)
def update_index_interval():
    global model
    try:
        if model.get_update_data():
            logger.debug('开始更新索引')
            ts = datetime.datetime.now().strftime("%H%M")
            model.update_all(ts)
            logger.debug('索引更新完成')
    except Exception as e:
        logger.error('更新索引出错 ' + str(e))
        raise JsonError(description=str(e), type=str(type(e).__name__))


def my_json(func):
    import functools

    @functools.wraps(func)
    def wrapper(*args, **kw):
        return json.dumps(func(*args, **kw), ensure_ascii=False)
    return wrapper


@app.route("/health", methods=["GET"])
def heath():
    return Response(json.dumps({"status":"ON"}), status=200, mimetype='application/json')


@app.route('/recall', methods=["POST"])
@my_json
def recall_top_k():
    data = request.form if request.form else request.json
    ret = {}
    try:
        logger.info('new request from %s' % request.remote_addr)
        vid = data.get('vid', -1)
        embed = data.get('embed', '')
        batch_mode = int(data.get('batch_mode', 0))
        include_distance = int(data.get('include_distance', 1))
        include_distance = True if include_distance == 1 else False
        recall_k = int(data.get('recall_k', 10))
        vid = str(vid)
        if not vid:
            logger.info('vid is none, return.')
            return ret

        if (batch_mode == 0 and type(vid) != str) or (batch_mode != 0 and type(vid) != list):
            logger.error('vid type and batch_mode not match, please check it')
            return ret

        if not embed:
            logger.error('embedding is none, please check it')
            return ret

        ret = model.search_by_vector(vid, embed, batch_mode=batch_mode, recall_k=recall_k, include_distances=include_distance)

        logger.info("vid is:{}, recall_k is:{}\n recall_k for vid is:{}".format(vid, recall_k, ret))
    except Exception as e:
        logger.error('error when handling HTTP request! ' + str(e), exc_info=True)
        raise JsonError(description=str(e), type=str(type(e).__name__))

    return ret


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5011)
