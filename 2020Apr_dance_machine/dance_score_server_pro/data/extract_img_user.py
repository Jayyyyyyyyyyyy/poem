import cv2
import os
import numpy as np

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
    for x in np.arange(40, 70, 1):
        for y in [-0.25,0,0.25]:
            z = x+y
            vidcap.set(cv2.CAP_PROP_POS_MSEC, (z * 1000))  # added this line
            _, image = vidcap.read()
            image = cv2.resize(image, (720, 450))
            z = z*100
            cv2.imwrite(image_output_dir_path + os.path.sep + "frame%d.jpg" % z, image)  # save frame as JPEG file
def main():
    print(cv2.__version__)
    video_url = 'http://aqiniudl.tangdou.com/202004/BF7879794E0D89F25AF533FA35127225.mp4'
    output_file = './user'
    extract_images(video_url, output_file)

if __name__ == '__main__':
    main()

