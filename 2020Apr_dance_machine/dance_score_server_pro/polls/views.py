from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt
import logging
import numpy as np
import requests
import json
import time
import math
import base64
import cv2
import os
import shutil
import re
from .img2kafka import Kafka_producer

class PortGenerator(object):
    def __init__(self):
        #self.ports = "8912,8913,8914,8915,8916"
        self.ports = "8912"
        self.alist = [val.strip() for val in self.ports.split(',')]
        self.length = len(self.alist)
        self.i = 0
        self.count = 0

    def traversal_list(self):
        while True:
            self.i = self.i % (self.length)
            yield self.alist[self.i]

    def traversal_ports(self):
        f = self.traversal_list()
        a = next(f)
        self.i += 1
        self.count += 1
        return (a, self.count)
myport = PortGenerator()


# def laod_teacher_keypoints(json_path):
#     g = os.walk(json_path)
#     teacher_frame_dict= {}
#     for path, dir_list, file_list in g:
#         for file_name in file_list:
#             my_path = os.path.join(path, file_name)
#             number = re.findall(r'\d+', my_path)[-1]
#             with open(my_path, 'r') as load_f:
#                 #res = json.load(load_f, encoding='utf-8')['people'][1]['pose_keypoints_2d']
#                 obj = json.load(load_f, encoding='utf-8')
#                 user_keypoint = obj['keypoints'][0]
#                 teacher_frame_dict[int(number)] = user_keypoint
#     return teacher_frame_dict
hard_frames = set([45, 49, 52, 55, 57, 62, 64, 67, 69, 72, 75, 78, 80, 83, 85, 87, 90, 92, 94, 96, 99, 101, 103, 139, 143, 146, 148, 152, 154, 156, 161, 163, 165, 169, 171, 173, 175, 177, 179, 182, 184, 186, 188, 190, 192, 194, 196, 200, 203, 205, 207, 209, 212, 215, 217, 219, 222, 224, 263, 267, 271, 273, 275, 278, 282, 285, 288, 291, 296, 298, 302, 304, 308, 312, 316, 319, 323, 327, 331, 334, 338, 342, 348, 354, 358, 361, 367, 368, 372, 383, 387, 390, 394, 398, 402, 406, 409, 413, 417, 420, 424, 428, 430, 432, 434, 460, 462, 464, 466, 468, 470, 478, 482, 485, 489, 522, 527, 531, 535, 539, 608, 612, 616, 655, 659, 663, 667, 675, 679, 683, 689, 691, 693, 695, 697, 699, 706, 708, 710, 713, 715, 717, 721, 725, 729, 732, 736, 740, 745, 769, 772, 775, 779, 783, 786, 790, 794, 797, 799, 801, 803, 807, 809, 829, 833, 837, 841, 845, 850, 852, 854, 856, 883, 886, 887, 891, 893, 897, 899, 957, 959, 961, 963, 965, 967, 982, 985, 989, 993, 997, 1003, 1008, 1011, 1015, 1019, 1023, 1027, 1030, 1036, 1039, 1043, 1046, 1052, 1055, 1059, 1063, 1099, 1103, 1107, 1111, 1115, 1119, 1123, 1127, 1131, 1134, 1137, 1140, 1143, 1147, 1150, 1153, 1156, 1159, 1267, 1271, 1275, 1277, 1281, 1288, 1291, 1299, 1303, 1307, 1311, 1314, 1326, 1330, 1333, 1336, 1338, 1340, 1342, 1344, 1349, 1351, 1353, 1355, 1357, 1359, 1364, 1368, 1372, 1375, 1379, 1382, 1384, 1386, 1388, 1390, 1393, 1395, 1397, 1399, 1401, 1403, 1405, 1408, 1412, 1417, 1421, 1424, 1428, 1432, 1436, 1440, 1444, 1447, 1451, 1457, 1461, 1464, 1466, 1470, 1472, 1474, 1476, 1478, 1480, 1485, 1487, 1489, 1491, 1493, 1495, 1499, 1501, 1502, 1504, 1507, 1509, 1514, 1516, 1518, 1520, 1521, 1523, 1528, 1530, 1532, 1534, 1536, 1538, 1540, 1546, 1550, 1553, 1556, 1560, 1564, 1568, 1571, 1606, 1608, 1610, 1612, 1614, 1616, 1618, 1620, 1623, 1625, 1627, 1628, 1630, 1632, 1638, 1640, 1642, 1644, 1645, 1648, 1654, 1658, 1662, 1666, 1669, 1673, 1677, 1681, 1686, 1690, 1693, 1697, 1701, 1704, 1707, 1711, 1716, 1718, 1720, 1722, 1724, 1726, 1728, 1730, 1732, 1736, 1740, 1744, 1746, 1750, 1753, 1758, 1762, 1764, 1769, 1772, 1775, 1778, 1781, 1784, 1787, 1791, 1794, 1798, 1801, 1805, 1807, 1810, 1814, 1816, 1820, 1823, 1827, 1832, 1836, 1839, 1842, 1845, 1849, 1854, 1857, 1866, 1871, 1877, 1893, 1917, 1958, 1962, 1968, 1973, 1982, 1987, 1994])
easy_frames = set([472, 494, 496, 499, 503, 507, 574, 578, 582, 586, 590, 594, 598, 602, 873, 876, 879, 1231, 1243, 1247, 1251, 1255, 1259])
hardest_frames = set([340, 344, 346, 376, 379, 436, 438, 440, 512, 514, 516, 518, 520, 901, 903, 906, 909, 913, 915, 917, 940, 944, 948, 951, 955, 970, 975, 979, 1318, 1322, 1887, 1911, 1926, 1934, 1940, 1947, 1951, 2003, 2008, 2013, 2017, 2021])

flip_frames = set([49, 52, 55, 57, 62, 64, 67, 69, 72, 75, 78, 80, 83, 85, 87, 90, 92, 94, 96, 99, 101, 103, 139, 143, 146, 148, 152, 154, 156, 161, 163, 165, 169, 171, 173, 175, 177, 179, 182, 184, 186, 188, 190, 192, 194, 196, 200, 203, 205, 207, 209, 212, 215, 217, 219, 222, 224, 263, 267, 271, 273, 275, 278, 282, 285, 288, 291, 296, 298, 302, 304, 308, 312, 316, 319, 323, 327, 331, 334, 367, 368, 372, 376, 379, 383, 387, 390, 394, 398, 402, 406, 409, 413, 417, 420, 424, 478, 482, 485, 489, 512, 514, 516, 518, 520, 522, 527, 531, 535, 539, 543, 545, 547, 549, 551, 553, 555, 559, 562, 564, 566, 568, 570, 574, 578, 582, 586, 590, 594, 598, 602, 608, 612, 616, 623, 625, 627, 629, 631, 633, 635, 639, 641, 643, 645, 647, 649, 651, 675, 679, 683, 706, 708, 710, 713, 715, 717, 736, 740, 769, 772, 775, 779, 783, 786, 790, 845, 850, 852, 854, 856, 876, 879, 883, 886, 887, 891, 893, 897, 899, 901, 903, 955, 957, 959, 961, 963, 965, 967, 985, 989, 993, 997, 1036, 1039, 1043, 1046, 1052, 1055, 1059, 1063, 1067, 1071, 1075, 1079, 1083, 1087, 1091, 1095, 1131, 1134, 1137, 1140, 1143, 1147, 1150, 1153, 1156, 1159, 1163, 1166, 1168, 1170, 1172, 1174, 1179, 1184, 1186, 1188, 1190, 1192, 1194, 1196, 1198, 1200, 1202, 1204, 1206, 1210, 1212, 1214, 1216, 1218, 1220, 1222, 1224, 1267, 1271, 1275, 1277, 1281, 1288, 1291, 1299, 1303, 1307, 1311, 1314, 1318, 1322, 1322, 1326, 1330, 1333, 1336, 1338, 1340, 1342, 1344, 1349, 1351, 1353, 1355, 1357, 1359, 1364, 1368, 1372, 1375, 1379, 1382, 1384, 1386, 1388, 1390, 1393, 1395, 1397, 1399, 1401, 1403, 1405, 1408, 1412, 1417, 1421, 1424, 1428, 1432, 1436, 1440, 1444, 1447, 1451, 1485, 1487, 1489, 1491, 1493, 1495, 1499, 1501, 1502, 1504, 1507, 1509, 1514, 1516, 1518, 1520, 1521, 1523, 1528, 1530, 1532, 1534, 1536, 1538, 1540, 1546, 1550, 1553, 1556, 1560, 1564, 1568, 1571, 1623, 1625, 1627, 1628, 1630, 1632, 1638, 1640, 1642, 1644, 1645, 1648, 1654, 1658, 1662, 1666, 1669, 1673, 1677, 1681, 1736, 1740, 1744, 1746, 1750, 1753, 1758, 1762, 1764, 1784, 1787, 1791, 1794, 1798, 1801, 1805, 1807, 1810, 1814, 1816, 1820, 1823, 1827, 1832, 1836, 1839, 1842, 1845, 1849, 1854, 1857, 1877, 1881, 1887, 1893, 1900, 1906, 1911, 1917, 1934, 1940, 1947, 1951, 1982, 1987])


# def laod_teacher_keypointsbak(json_path):
#     g = os.walk(json_path)
#     teacher_frame_dict= {}
#     for path, dir_list, file_list in g:
#         for file_name in file_list:
#             my_path = os.path.join(path, file_name)
#             number = re.findall(r'\d+', my_path)[-1]
#             with open(my_path, 'r') as load_f:
#                 res = json.load(load_f, encoding='utf-8')['people'][1]['pose_keypoints_2d']
#                 new_list = [res[i:i + 3] for i in range(0, len(res), 3)]
#                 teacher_frame_dict[int(number)] = new_list
#     return teacher_frame_dict
# teachers_path = './data/output_json_teacher'
# teacher_frame_dict = laod_teacher_keypoints(teachers_path)

def laod_teacher_keypoints(json_path):
    g = os.walk(json_path)
    teacher_frame_dict = {}
    for path, dir_list, file_list in g:
        for file_name in file_list:
            my_path = os.path.join(path, file_name)
            father = int(re.findall(r'\d+', my_path)[-2])
            if father not in teacher_frame_dict:
                teacher_frame_dict[father] = {}
            son = int(re.findall(r'\d+', my_path)[-1])
            with open(my_path, 'r') as load_f:
                # res = json.load(load_f, encoding='utf-8')['people'][1]['pose_keypoints_2d']
                obj = json.load(load_f, encoding='utf-8')
                user_keypoint = obj['keypoints'][0]
                teacher_frame_dict[father][son] = user_keypoint
    return teacher_frame_dict


teachers_path = './data/output_json_teacher'
teacher_frame_dict = laod_teacher_keypoints(teachers_path)
teachers_path_flip = './data/output_json_teacher_flip_pro'
teacher_frame_dict_flip = laod_teacher_keypoints(teachers_path_flip)
logging.info("frames:{}".format(teacher_frame_dict.keys()))
def vec(orig, landmark):
    return [orig[0], orig[1], landmark[0], landmark[1]]

def score_pro(teacher_time, user_keypoint, my_teacher_frame_dict, flag=None):
    mid_store = []
    for key in my_teacher_frame_dict[teacher_time].keys():
        teacher_keypoint = my_teacher_frame_dict[teacher_time][key]
        tmp_res = my_score(user_keypoint, teacher_keypoint, teacher_time)
        tmp_res.append(key)
        mid_store.append(tmp_res)
    s, avg, key = min(mid_store, key=lambda x: x[1])
    if flag:
        logging.info('{}: score:{}, avg:{}, key_org:{}'.format(flag, s, avg, key))
    return (s, avg, key)
def angle(v1, v2):
  dx1 = v1[2] - v1[0]
  dy1 = v1[3] - v1[1]
  dx2 = v2[2] - v2[0]
  dy2 = v2[3] - v2[1]
  angle1 = math.atan2(dy1, dx1)
  angle1 = int(angle1 * 180/math.pi)
  # print(angle1)
  angle2 = math.atan2(dy2, dx2)
  angle2 = int(angle2 * 180/math.pi)
  # print(angle2)
  if angle1*angle2 >= 0:
    included_angle = abs(angle1-angle2)
  else:
    included_angle = abs(angle1) + abs(angle2)
    if included_angle > 180:
      included_angle = 360 - included_angle
  return included_angle


def getByte(path):
    with open(path, 'rb') as f:
        img_byte = base64.b64encode(f.read())
    img_str = img_byte.decode('ascii')
    return img_str


def keypoints(jsonbody,port):
    #url = "http://10.19.135.159:{}/polls/keypoints".format(port)
    url = "http://10.19.6.41:8901/polls/keypoints".format(port)
    resp1 = requests.post(url, data=jsonbody)
    return resp1.text

# def getAngleBetweenPoints(orig, landmark):
#     x_orig, y_orig = orig[:2]
#     x_landmark, y_landmark = landmark[:2]
#     deltaY = y_landmark - y_orig
#     deltaX = x_landmark - x_orig
#     angleInDegrees = atan2(deltaY, deltaX) * 180 / pi
#     return angleInDegrees + 180


def distance(user_keypoint):
    user = {}
    user['Nose'] = user_keypoint[0]
    user['Neck'] = user_keypoint[1]
    user['RShoulder'] = user_keypoint[2]
    user['RElbow'] = user_keypoint[3]
    user['RWrist'] = user_keypoint[4]
    user['LShoulder'] = user_keypoint[5]
    user['LElbow'] = user_keypoint[6]
    user['LWrist'] = user_keypoint[7]
    user['MidHip'] = user_keypoint[8]
    user['RHip'] = user_keypoint[9]
    user['RKnee'] = user_keypoint[10]
    user['RAnkle'] = user_keypoint[11]
    user['LHip'] = user_keypoint[12]
    user['LKnee'] = user_keypoint[13]
    user['LAnkle'] = user_keypoint[14]

    detected_points = 0
    for x in user_keypoint[:15]:
        if x == [0.0, 0.0, 0.0]:
            detected_points += 1
    if detected_points == 15:
        distance = 4
    else:
        if user['LKnee'] != [0.0, 0.0, 0.0] or user['RKnee'] != [0.0, 0.0, 0.0]:
            distance = 1
        # if user['MidHip'] != [0.0, 0.0, 0.0]:
        #     distance = 1
        else:
            tmp_flag = True
            undetected_points = 0
            for x in user_keypoint[:8]:
                if x == [0.0, 0.0, 0.0]:
                    undetected_points += 1
                    tmp_flag = False
            if tmp_flag == True:
                distance = 2
            else:
                if undetected_points >= 3:
                    distance = 3
                else:
                    distance = 2
    return distance

def measure_distance(user_keypoint):
    # user_keypoint[1] # neck
    # user_keypoint[3] # relbow
    # user_keypoint[4] # rwrist
    # user_keypoint[6] # lelbow
    # user_keypoint[7] # lwrist

    valid_index = [index for index, value in enumerate([user_keypoint[3],user_keypoint[4],user_keypoint[6],user_keypoint[7]]) if value != [0.0, 0.0, 0.0]]
    if user_keypoint[1] == [0.0, 0.0, 0.0] or len(valid_index) < 2:
        return 4
    else:
        return -1

def my_score(user_keypoint, teacher_keypoint, teacher_time):
    tmp_list = [user_keypoint[3],user_keypoint[4],user_keypoint[6],user_keypoint[7]]
    valid_index = [index for index, value in enumerate(tmp_list) if value != [0.0, 0.0, 0.0]]
    user = {}
    user['Nose'] = user_keypoint[0]
    user['Neck'] = user_keypoint[1]
    user['RShoulder'] = user_keypoint[2]
    user['RElbow'] = user_keypoint[3]
    user['RWrist'] = user_keypoint[4]
    user['LShoulder'] = user_keypoint[5]
    user['LElbow'] = user_keypoint[6]
    user['LWrist'] = user_keypoint[7]

    if user['Neck'] == [0.0, 0.0, 0.0]: #or detected_points <=3:
        return 0, 0
    teacher = {}
    teacher['Nose'] = teacher_keypoint[0]
    teacher['Neck'] = teacher_keypoint[1]
    teacher['RShoulder'] = teacher_keypoint[2]
    teacher['RElbow'] = teacher_keypoint[3]
    teacher['RWrist'] = teacher_keypoint[4]
    teacher['LShoulder'] = teacher_keypoint[5]
    teacher['LElbow'] = teacher_keypoint[6]
    teacher['LWrist'] = teacher_keypoint[7]
    # teacher['MidHip'] = teacher_keypoint[8]
    # teacher['RHip'] = teacher_keypoint[9]
    # teacher['RKnee'] = teacher_keypoint[10]
    # teacher['RAnkle'] = teacher_keypoint[11]
    # teacher['LHip'] = teacher_keypoint[12]
    # teacher['LKnee'] = teacher_keypoint[13]
    # teacher['LAnkle'] = teacher_keypoint[14]

    if user['LWrist'] != [0.0, 0.0, 0.0] and user['RWrist'] != [0.0, 0.0, 0.0] and user['LElbow'] != [0.0, 0.0, 0.0] and user['RElbow'] != [0.0, 0.0, 0.0]:
        if (user['LWrist'][0] - user['RWrist'][0] > 0 and teacher['LWrist'][0] - teacher['RWrist'][0] > 0) or (user['LWrist'][0] - user['RWrist'][0] < 0 and teacher['LWrist'][0] - teacher['RWrist'][0] < 0):
            teacher_vector_1 = vec(teacher['Neck'], teacher['Nose'])
            teacher_vector_2 = vec(teacher['Neck'], teacher['RWrist'])
            teacher_vector_3 = vec(teacher['Neck'], teacher['LWrist'])
            teacher_vector_4 = vec(teacher['Neck'], teacher['RShoulder'])
            teacher_vector_5 = vec(teacher['Neck'], teacher['LShoulder'])
            teacher_vector_6 = vec(teacher['Neck'], teacher['RElbow'])
            teacher_vector_7 = vec(teacher['Neck'], teacher['LElbow'])
            user_vector_1 = vec(user['Neck'], user['Nose'])
            user_vector_2 = vec(user['Neck'], user['RWrist'])
            user_vector_3 = vec(user['Neck'], user['LWrist'])
            user_vector_4 = vec(user['Neck'], user['RShoulder'])
            user_vector_5 = vec(user['Neck'], user['LShoulder'])
            user_vector_6 = vec(user['Neck'], user['RElbow'])
            user_vector_7 = vec(user['Neck'], user['LElbow'])
        else:
            teacher_vector_1 = vec(teacher['Neck'], teacher['Nose'])
            teacher_vector_2 = vec(teacher['Neck'], teacher['RWrist'])
            teacher_vector_3 = vec(teacher['Neck'], teacher['LWrist'])
            teacher_vector_4 = vec(teacher['Neck'], teacher['RShoulder'])
            teacher_vector_5 = vec(teacher['Neck'], teacher['LShoulder'])
            teacher_vector_6 = vec(teacher['Neck'], teacher['RElbow'])
            teacher_vector_7 = vec(teacher['Neck'], teacher['LElbow'])
            user_vector_1 = vec(user['Neck'], user['Nose'])
            user_vector_2 = vec(user['Neck'], user['LWrist'])
            user_vector_3 = vec(user['Neck'], user['RWrist'])
            user_vector_4 = vec(user['Neck'], user['LShoulder'])
            user_vector_5 = vec(user['Neck'], user['RShoulder'])
            user_vector_6 = vec(user['Neck'], user['LElbow'])
            user_vector_7 = vec(user['Neck'], user['RElbow'])
    else:
        teacher_vector_1 = vec(teacher['Neck'], teacher['Nose'])
        teacher_vector_2 = vec(teacher['Neck'], teacher['RWrist'])
        teacher_vector_3 = vec(teacher['Neck'], teacher['LWrist'])
        teacher_vector_4 = vec(teacher['Neck'], teacher['RShoulder'])
        teacher_vector_5 = vec(teacher['Neck'], teacher['LShoulder'])
        teacher_vector_6 = vec(teacher['Neck'], teacher['RElbow'])
        teacher_vector_7 = vec(teacher['Neck'], teacher['LElbow'])
        user_vector_1 = vec(user['Neck'], user['Nose'])
        user_vector_2 = vec(user['Neck'], user['RWrist'])
        user_vector_3 = vec(user['Neck'], user['LWrist'])
        user_vector_4 = vec(user['Neck'], user['RShoulder'])
        user_vector_5 = vec(user['Neck'], user['LShoulder'])
        user_vector_6 = vec(user['Neck'], user['RElbow'])
        user_vector_7 = vec(user['Neck'], user['LElbow'])

    delta_angle_1 = angle(teacher_vector_1, user_vector_1)
    delta_angle_2 = angle(teacher_vector_2, user_vector_2)
    delta_angle_3 = angle(teacher_vector_3, user_vector_3)
    delta_angle_4 = angle(teacher_vector_4, user_vector_4)
    delta_angle_5 = angle(teacher_vector_5, user_vector_5)
    delta_angle_6 = angle(teacher_vector_6, user_vector_6)
    delta_angle_7 = angle(teacher_vector_7, user_vector_7)
    logging.info('angles')
    logging.info([delta_angle_2, delta_angle_3, delta_angle_6, delta_angle_7])
    valid_angles = list(np.array([delta_angle_2, delta_angle_3, delta_angle_6, delta_angle_7])[valid_index])
    if len(valid_angles) == 0:
        score = 0
        avg = 0
    else:
        avg = np.mean(valid_angles)
        details ="valid_index:{}\nvalid angle:{}\nfinal_avg:{}".format(valid_index, valid_angles, avg)
        logging.info(details)

        if teacher_time in hard_frames:
            if avg <= 10 and avg >= 1:
                score = 100
            elif avg <= 20 and avg > 10:
                score = 90
            elif avg <= 40 and avg > 20:
                score = 80
            else:
                score = 0

        elif teacher_time in hardest_frames:
            if avg <= 15 and avg >= 1:
                score = 100
            elif avg <= 30 and avg > 15:
                score = 90
            elif avg <= 50 and avg > 30:
                score = 80
            else:
                score = 0
        elif teacher_time in easy_frames:
            if avg <= 7 and avg >= 1:
                score = 100
            elif avg <= 15 and avg > 7:
                score = 90
            elif avg <= 25 and avg > 15:
                score = 80
            else:
                score = 0
        else:
            if avg <= 8 and avg >= 1:
                score = 100
            elif avg <= 16 and avg > 8:
                score = 90
            elif avg <= 30 and avg > 16:
                score = 80
            else:
                score = 0


    return [score, avg]


def write_img(img, path, points):
    img_decode = base64.b64decode(img)  # base64解~A
    img_np = np.frombuffer(img_decode, np.uint8)  # NbyteMO~V为np.array形O
    img = cv2.imdecode(img_np, cv2.COLOR_RGB2BGR)
    ptStart = (int(points[0][0]),int(points[0][1]))
    ptEnd = (int(points[1][0]),int(points[1][1]))
    point_color = (0, 255, 0)  # BGR
    thickness = 1
    img = cv2.line(img, ptStart, ptEnd, point_color, thickness)
    ptStart = (int(points[0][0]),int(points[0][1]))
    ptEnd = (int(points[2][0]),int(points[2][1]))
    point_color = (0, 0, 255)  # BGR
    img = cv2.line(img, ptStart, ptEnd, point_color, thickness)
    ptStart = (int(points[0][0]),int(points[0][1]))
    ptEnd = (int(points[3][0]),int(points[3][1]))
    point_color = (255, 0, 0)  # BGR
    img = cv2.line(img, ptStart, ptEnd, point_color, thickness)
    ptStart = (int(points[0][0]),int(points[0][1]))
    ptEnd = (int(points[4][0]),int(points[4][1]))
    point_color = (50, 50, 50)  # BGR
    img = cv2.line(img, ptStart, ptEnd, point_color, thickness)
    cv2.imwrite(path, img)


def stage2_flip(img, tmp_port, teacher_time, uid, timestamp):
    temp_res = ''
    user_parms = {"img": img}
    obj = keypoints(user_parms, tmp_port)
    obj = json.loads(obj)
    if obj['code'] == 2:
        result = {'score': 0, 'distance': 4}
        result['code'] = 1
        result['uid'] = uid
        result['upload_time'] = timestamp
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info(responsejson + '没有检测到人物')
        user_keypoint, teacher_keypoint = None, None
        path = None
    else:
        user_keypoint = obj['keypoints'][0]
        s_org, avg_org, key_org = score_pro(teacher_time, user_keypoint, teacher_frame_dict, 'org')
        s_flip, avg_flip, key_flip = score_pro(teacher_time, user_keypoint, teacher_frame_dict_flip, 'flip')
        s, avg, flag_from = min(zip([s_org, s_flip],[avg_org, avg_flip], ['org', 'flip']), key=lambda x: x[1])
        if flag_from == 'org':
            teacher_keypoint = teacher_frame_dict[teacher_time][key_org]
            path = './data/teacher_pro/frame_{}_{}.jpg'.format(teacher_time, key_org)
        if flag_from == 'flip':
            teacher_keypoint = teacher_frame_dict_flip[teacher_time][key_flip]
            path = './data/teacher_pro_flip/frame_{}_{}_flip.jpg'.format(teacher_time, key_flip)
        temp_res = '###frame_result####\n flag_from:{}, score:{}, avg:{}, frame_num:{}, timestamp:{}\nuser_keypoints:{}\nteacher_keypoints:{}'.format(flag_from, s, avg, teacher_time, timestamp,list(np.array(user_keypoint)[[1,3,4,6,7]]), list(np.array(teacher_keypoint)[[1,3,4,6,7]]))
        logging.info(temp_res)
        distance = measure_distance(user_keypoint)
        result = {'score': s, 'distance': distance}
        result['code'] = 1
        result['uid'] = uid
        result['upload_time'] = timestamp
    return result, user_keypoint, teacher_keypoint, temp_res, path

def stage2(img,tmp_port, teacher_time, uid, timestamp):
    temp_res = ''
    user_parms = {"img": img}
    obj = keypoints(user_parms, tmp_port)
    obj = json.loads(obj)
    if obj['code'] == 2:
        result = {'score': 0, 'distance': 4}
        result['code'] = 1
        result['uid'] = uid
        result['upload_time'] = timestamp
        responsejson = json.dumps(result, ensure_ascii=False)
        logging.info(responsejson + '没有检测到人物')
        user_keypoint, teacher_keypoint = None, None
        path = None
    else:
        user_keypoint = obj['keypoints'][0]
        mid_store = []
        for key in teacher_frame_dict[teacher_time].keys():
            teacher_keypoint = teacher_frame_dict[teacher_time][key]
            tmp_res = my_score(user_keypoint, teacher_keypoint, teacher_time)
            tmp_res.append(key)
            mid_store.append(tmp_res)

        s, avg, key = min(mid_store, key=lambda x: x[1])
        path = './data/teacher_pro/frame_{}_{}.jpg'.format(teacher_time, key)
        temp_res = '###frame_result####\nscore:{}, avg:{}, frame_num:{}, which_frame:{}, timestamp:{}\nuser_keypoints:{}\nteacher_keypoints:{}'.format(s, avg, teacher_time, key, timestamp,list(np.array(user_keypoint)[[1,3,4,6,7]]), list(np.array(teacher_keypoint)[[1,3,4,6,7]]))
        logging.info(temp_res)
        distance = measure_distance(user_keypoint)
        result = {'score': s, 'distance': distance}
        result['code'] = 1
        result['uid'] = uid
        result['upload_time'] = timestamp
    return result, user_keypoint, teacher_keypoint, temp_res, path


#producer = Kafka_producer("10.19.17.74:9092,10.19.130.22:9092,10.19.11.29:9092", 9092, "dance_machine")


@csrf_exempt
def score(request):
    if request.method == "POST":
        uid = request.POST.get('uid', '')
        img = request.POST.get('user_imgs', '')
        teacher_time = request.POST.get('teacher_frame_time', '')
        flag = request.POST.get('flag', '')
        timestamp = request.POST.get('upload_time', '')
    elif request.method == "GET":
        uid = request.GET.get('uid', '')
        img = request.GET.get('user_imgs', '')
        teacher_time = request.GET.get('teacher_frame_time', '')
        flag = request.GET.get('flag', '')
        timestamp = request.GET.get('upload_time', '')
    try:
        logging.info("Request:{}, {}, {}, {}, {}".format(uid,teacher_time,flag,img[:20],timestamp))
        flag = int(flag)
        teacher_time = int(teacher_time)
        start = time.time()
        tmp_port, c = myport.traversal_ports()

        if flag == 1:
            user_parms = {"img": img}
            obj = keypoints(user_parms, tmp_port)
            logging.info("obj:{}".format(obj))
            obj = json.loads(obj)
            if obj['code'] == 2:
                result = {'score': -1, 'distance': 4}
            else:
                if len(obj['keypoints']) == 1:
                    user_keypoint = obj['keypoints'][0]
                    my_distance = distance(user_keypoint)
                    result = {'score': -1, 'distance': my_distance}
                else:
                    tmp_persons=[]
                    for person in obj['keypoints']:
                        detected_points = 0
                        for point in person[:8]:
                            if point == [0.0, 0.0, 0.0]:
                                detected_points += 1
                        tmp_persons.append(detected_points)
                    ind = tmp_persons.index(min(tmp_persons))
                    user_keypoint = obj['keypoints'][ind]
                    my_distance = distance(user_keypoint)
                    result = {'score': -1, 'distance': my_distance}
            result['uid'] = uid
            result['upload_time'] = timestamp
            responsejson = json.dumps(result, ensure_ascii=False)
            logging.info("Response:" + responsejson)
            return HttpResponse(responsejson)
            ##########  tmp, will delete ###########
            # path_parent = './tmp/{}'.format(uid)
            # if not os.path.exists(path_parent):
            #     os.mkdir(path_parent)
            # path_son = "{}/{}_{}".format(path_parent, teacher_time, my_distance)
            # if not os.path.exists(path_son):
            #     os.mkdir(path_son)
            # user_image_path = "{}/user.png".format(path_son)
            # write_img(img, user_image_path)
            ##########  tmp, will delete ###########
        else:
            if teacher_time in flip_frames:
                result, user_keypoint, teacher_keypoint, temp_res, teacher_path = stage2_flip(img, tmp_port, teacher_time, uid, timestamp)
            else:
                result, user_keypoint, teacher_keypoint, temp_res, teacher_path = stage2(img, tmp_port, teacher_time, uid, timestamp)


            # kafka_res = {}
            # kafka_res['uid'] = uid
            # kafka_res['teacher_frame_time'] = teacher_time
            # kafka_res['upload_time'] = timestamp
            # if 'score' in result:
            #     kafka_res['score'] = result['score']
            # else:
            #     kafka_res['score'] = 0
            # producer.sendjsondata(kafka_res, "{}_{}".format(uid,timestamp))


            responsejson = json.dumps(result, ensure_ascii=False)
            logging.info("Response:" + responsejson)
            end = time.time()
            logging.info("OpenPose demo successfully finished. Total time: " + str(end - start) + " seconds")
            #####  log imgs to local for analysis test start
            if user_keypoint is not None and teacher_keypoint is not None and teacher_path is not None:
               path_parent = './tmp/{}'.format(uid)
               if not os.path.exists(path_parent):
                   os.mkdir(path_parent)
               path_son = "{}/{}_{}".format(path_parent, teacher_time, timestamp)
               if not os.path.exists(path_son):
                   os.mkdir(path_son)
               text_path = "{}/{}.txt".format(path_son,teacher_time)
               with open(text_path,'w') as writer:
                   writer.write(temp_res)
               user_image_path = "{}/user{}.jpg".format(path_son,teacher_time)
               #infile = "./data/teacher/frame{}.jpg".format(teacher_time)
               # infile = path
               outfile = "{}/{}".format(path_son,teacher_path.split('/')[-1])
               shutil.copy(teacher_path, outfile)
               write_img(img, user_image_path, list(np.array(user_keypoint)[[1,3,4,6,7]]))
            #####  log imgs to local for analysis test end
            return HttpResponse(responsejson)

        #
        #     user_parms = {"img": img}
        #     obj = keypoints(user_parms, tmp_port)
        #     logging.info('org_user_keypoints:{}'.format(obj))
        #     obj = json.loads(obj)
        #     #teacher_img_path = './data/teacher/frame{}.jpg'.format(teacher_time)
        #     # teacher_img = cv2.imread(teacher_img_path)
        #     #teacher_img = getByte(teacher_img_path)
        #     if obj['code'] == 2:
        #         result = {'code': 2}
        #         responsejson = json.dumps(result, ensure_ascii=False)
        #         logging.info(responsejson +'没有检测到人物')
        #         s = 0
        #         #return HttpResponse(responsejson)
        #     else:
        #         user_keypoint = obj['keypoints'][0]
        #         teacher_keypoint = teacher_frame_dict[teacher_time]
        #         s = my_score(user_keypoint,teacher_keypoint)
        #         # logging.info('score:{}'.format(s))
        #         # logging.info('teacher_keypoint:{}'.format(teacher_keypoint))
        #         # logging.info('user_keypoint:{}'.format(user_keypoint))
        #         # path_parent = './tmp/{}'.format(uid)
        #         # if not os.path.exists(path_parent):
        #         #     os.mkdir(path_parent)
        #         # path_son = "{}/{}_{}_{}".format(path_parent, teacher_time, s, time.time())
        #         # if not os.path.exists(path_son):
        #         #     os.mkdir(path_son)
        #         # user_image_path = "{}/user.png".format(path_son)
        #         # write_img(img, user_image_path)
        #
        #
        #     result = {'score': s, 'distance': -1}
        # end = time.time()
        # logging.info("OpenPose demo successfully finished. Total time: " + str(end - start) + " seconds")
        # result['code'] = 1
        # responsejson = json.dumps(result, ensure_ascii=False)
        # logging.info("Response:" + responsejson)
        # return HttpResponse(responsejson)
    except Exception as e:
        logging.error("querytag exception: " + e)
        # result = {'code':2}
        # responsejson = json.dumps(result, ensure_ascii=False)
        # logging.info(responsejson+'305')
        # return HttpResponse(responsejson)

