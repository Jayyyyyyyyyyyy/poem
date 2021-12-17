import json
import cv2
import os
import sys
sys.path.append('/root/data/openpose/build/python');
from openpose import pyopenpose as op

params = dict()
params["model_folder"] = "/root/data/keypoint_server_8912/models/"
params["number_people_max"] = 1
#params["net_resolution"] = "-1x288"

opWrapper = op.WrapperPython()
opWrapper.configure(params)
opWrapper.start()
datum = op.Datum()


dir = './teacher'
json_path = './output_json_teacher'
filenames = os.listdir(dir)

for filename in filenames:
    file_path = os.path.join(dir, filename)
    result = dict()
    img = cv2.imread(file_path, cv2.COLOR_RGB2BGR)
    imageToProcess = img
    # imageToProcess = cv2.imread(img)
    datum.cvInputData = imageToProcess
    opWrapper.emplaceAndPop([datum])
    tmp = datum.poseKeypoints.tolist()
    result['keypoints'] = tmp
    responsejson = json.dumps(result, ensure_ascii=False)
    filename = os.path.join(json_path, filename)
    with open(filename, 'w') as writer:
        writer.write(responsejson)


