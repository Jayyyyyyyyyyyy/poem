# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/5/22 10:17 AM
# @File      : gaussian_blur.py
# @Software  : PyCharm
# @Company   : Xiao Tang


# # This is the command line to be implemented
# # Scripts and 'imagefilter. Image' will be imported from the PIL
# from PIL import ImageFilter, Image
#
# # Note: In this case the image and the 19.03.2018.2345.py are present in the same directory. Therefore I need not to copy the full path for the cause.
# # The image format can vary within JPG/JPEG, PNG and GIF. SVG is under process, for that you can import svglib.svglib
imagename = "/Users/tangdou1/PycharmProjects/poem/小工具/Miyoshi_Ayaka/lena.jpg"
# # Now open the file wrt to the name of the file
# image = Image.open(imagename)
#
# # Now we're gonna use Gaussian Blur to blur the image
# imageBlur = image.filter(ImageFilter.GaussianBlur(radius=3))
# # Radius simply specifies the amount of blur you want in the picture
# # You can adjust it as per shown in the readme.
# imageBlur.show()  # This will just display the blurred image in the .BMP image format
#
# # Run the Module. A new file with the specified name will be generated. Adios!
from skimage import io
from skimage import filters
from PIL import Image
import numpy as np
img = io.imread(imagename)
io.imshow(img)
out = filters.gaussian(img, sigma=5)
out = np.array(out*255, dtype=np.uint8)
img = Image.fromarray(out)
img.show()
# io.imshow(out)
# io.show()