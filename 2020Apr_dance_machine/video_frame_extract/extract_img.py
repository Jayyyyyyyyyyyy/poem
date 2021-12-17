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
    df = pd.read_excel('/Users/jiangcx/PycharmProjects/poem/2020Apr_dance_machine/video_frame_extract/timeline.xlsx')

    tmp = []
    for index, row in df.iterrows() :
        min, sec = row[1].split(':')
        secs = int(min) * 60 + int(sec)
        tmp.append(secs)
        frame_num = secs * fps
        vidcap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
        #vidcap.set(cv2.CAP_PROP_POS_MSEC, (x * 1000))  # added this line
        _, image = vidcap.read()
        image = cv2.resize(image, (720, 450),interpolation=cv2.INTER_CUBIC)
        #image = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
        cv2.imwrite(image_output_dir_path + os.path.sep + "frame%d.jpg" % secs, image)  # save frame as JPEG file
    res = "|".join([str(x) for x in tmp])
    f = open('./timeline_for_server.txt', 'w')
    f.write(res)
    f.close()
    #cd
def main():
    print(cv2.__version__)
    video_url = './dance.mp4'
    output_file = './teacher'
    extract_images(video_url, output_file)

if __name__ == '__main__':
    main()


