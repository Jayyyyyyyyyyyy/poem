import cv2
import os
import pandas as pd

def extract_images(video_input_file_path, image_output_dir_path):
    print(image_output_dir_path)
    print('Extracting frames from video: ', video_input_file_path)
    print(video_input_file_path)
    vidcap = cv2.VideoCapture(video_input_file_path)
    fps = vidcap.get(cv2.CAP_PROP_FPS)
    print('fps is :' + str(fps))
    frameCount = int(vidcap.get(cv2.CAP_PROP_FRAME_COUNT))
    print(frameCount)
    duration = int(frameCount / fps)
    print(duration)
    #df = pd.read_excel('/Users/jiangcx/PycharmProjects/poem/2020Apr_dance_machine/video_frame_extract/timeline.xlsx')
    flip_list = [49, 52, 55, 57, 62, 64, 67, 69, 72, 75, 78, 80, 83, 85, 87, 90, 92, 94, 96, 99, 101, 103, 139, 143,
                 146, 148, 152, 154, 156, 161, 163, 165, 169, 171, 173, 175, 177, 179, 182, 184, 186, 188, 190, 192,
                 194, 196, 200, 203, 205, 207, 209, 212, 215, 217, 219, 222, 224, 263, 267, 271, 273, 275, 278, 282,
                 285, 288, 291, 296, 298, 302, 304, 308, 312, 316, 319, 323, 327, 331, 334, 367, 368, 372, 376, 379,
                 383, 387, 390, 394, 398, 402, 406, 409, 413, 417, 420, 424, 478, 482, 485, 489, 512, 514, 516, 518,
                 520, 522, 527, 531, 535, 539, 543, 545, 547, 549, 551, 553, 555, 559, 562, 564, 566, 568, 570, 574,
                 578, 582, 586, 590, 594, 598, 602, 608, 612, 616, 623, 625, 627, 629, 631, 633, 635, 639, 641, 643,
                 645, 647, 649, 651, 675, 679, 683, 706, 708, 710, 713, 715, 717, 736, 740, 769, 772, 775, 779, 783,
                 786, 790, 845, 850, 852, 854, 856, 876, 879, 883, 886, 887, 891, 893, 897, 899, 901, 903, 955, 957,
                 959, 961, 963, 965, 967, 985, 989, 993, 997, 1036, 1039, 1043, 1046, 1052, 1055, 1059, 1063, 1067,
                 1071, 1075, 1079, 1083, 1087, 1091, 1095, 1131, 1134, 1137, 1140, 1143, 1147, 1150, 1153, 1156, 1159,
                 1163, 1166, 1168, 1170, 1172, 1174, 1179, 1184, 1186, 1188, 1190, 1192, 1194, 1196, 1198, 1200, 1202,
                 1204, 1206, 1210, 1212, 1214, 1216, 1218, 1220, 1222, 1224, 1267, 1271, 1275, 1277, 1281, 1288, 1291,
                 1299, 1303, 1307, 1311, 1314, 1318, 1322, 1322, 1326, 1330, 1333, 1336, 1338, 1340, 1342, 1344, 1349,
                 1351, 1353, 1355, 1357, 1359, 1364, 1368, 1372, 1375, 1379, 1382, 1384, 1386, 1388, 1390, 1393, 1395,
                 1397, 1399, 1401, 1403, 1405, 1408, 1412, 1417, 1421, 1424, 1428, 1432, 1436, 1440, 1444, 1447, 1451,
                 1485, 1487, 1489, 1491, 1493, 1495, 1499, 1501, 1502, 1504, 1507, 1509, 1514, 1516, 1518, 1520, 1521,
                 1523, 1528, 1530, 1532, 1534, 1536, 1538, 1540, 1546, 1550, 1553, 1556, 1560, 1564, 1568, 1571, 1623,
                 1625, 1627, 1628, 1630, 1632, 1638, 1640, 1642, 1644, 1645, 1648, 1654, 1658, 1662, 1666, 1669, 1673,
                 1677, 1681, 1736, 1740, 1744, 1746, 1750, 1753, 1758, 1762, 1764, 1784, 1787, 1791, 1794, 1798, 1801,
                 1805, 1807, 1810, 1814, 1816, 1820, 1823, 1827, 1832, 1836, 1839, 1842, 1845, 1849, 1854, 1857, 1877,
                 1881, 1887, 1893, 1900, 1906, 1911, 1917, 1934, 1940, 1947, 1951, 1982, 1987]
    df = pd.DataFrame(flip_list)
    tmp = []
    for ind, secs in df.iterrows() :
        secs = secs[0]
        tmp.append(secs)
        frame_nums = secs * fps
        for index, frame_num in enumerate([frame_nums-8, frame_nums, frame_nums+8]):
            vidcap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
            #vidcap.set(cv2.CAP_PROP_POS_MSEC, (x * 1000))  # added this line
            _, image = vidcap.read()
            image = cv2.resize(image, (720, 450),interpolation=cv2.INTER_CUBIC)
            image = cv2.flip(image, 1)
            filename = "frame_{}_{}_flip.jpg".format(secs, index)
            print(filename)
            cv2.imwrite(os.path.join(image_output_dir_path,filename), image)  # save frame as JPEG file
    res = "|".join([str(x) for x in tmp])
    f = open('./timeline_for_server.txt', 'w')
    f.write(res)
    f.close()
    #cd
def main():
    print(cv2.__version__)
    video_url = './dance.mp4'
    output_file = './teacher_pro_flip'
    extract_images(video_url, output_file)

if __name__ == '__main__':
    main()


