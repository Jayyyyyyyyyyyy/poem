from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import logging
import json
import cv2
import time
import urllib.request
import numpy as np
import base64
import sys
sys.path.append('/root/data/openpose/build/python');
from openpose import pyopenpose as op

def my_open(URL):
    if 'http' in URL:
        with urllib.request.urlopen(URL) as url:
            image = np.asarray(bytearray(url.read()), dtype="uint8")
            image = cv2.imdecode(image, cv2.IMREAD_COLOR)
        return image
    else:
        return URL

params = dict()
params["model_folder"] = "./models/"
params["number_people_max"] = 1
#params["net_resolution"] = "-1x288"

opWrapper = op.WrapperPython()
opWrapper.configure(params)
opWrapper.start()
datum = op.Datum()
@csrf_exempt
def keypoints(request):
    if request.method == "POST":
        img = request.POST.get('img', '')
    elif request.method == "GET":
        img = request.GET.get('img', '')
    try:
        start = time.time()
        result = dict()
        #if 'http' in img:
        #    imageToProcess = my_open(img)
        #else:
        img_decode = base64.b64decode(img)  # base64解~A
        img_np = np.frombuffer(img_decode, np.uint8)  # NbyteMO~V为np.array形O
        img = cv2.imdecode(img_np, cv2.COLOR_RGB2BGR)
        imageToProcess = img
        # imageToProcess = cv2.imread(img)
        datum.cvInputData = imageToProcess
        opWrapper.emplaceAndPop([datum])
        tmp = datum.poseKeypoints.tolist()
        logging.info("type:{},array:{}".format(type(tmp), tmp))
        if type(tmp) != type([]):
            result['code'] = 2
        else:
            result['code'] = 1
            result['keypoints'] = tmp
        end = time.time()
        logging.info("OpenPose demo successfully finished. Total time: " + str(end - start) + " seconds")
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info("Response:" + responsejson)
        return HttpResponse(responsejson)
    except Exception as e:
        logging.error("querytag exception: " + e)