# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/5/22 5:54 PM
# @File      : gaussian_blur2.py.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import matplotlib.pyplot as plt
import numpy as np
from math import ceil
from PIL import Image
def load_image(filename):
    """Loads the provided image file, and returns it as a numpy array."""
    im = Image.open(filename)
    im.show()
    return np.array(im)


miyoshi = load_image("./Miyoshi_Ayaka/lena.jpg")
#plt.savefig('Miyoshi.jpg')
def convolutiond2d(input, filter, bias=0, strides=(1,1), padding='SAME'):
    input = input/255
    if not len(filter.shape) == 3:
        raise ValueError("The size of the filter should be (filter_height, filter_width, filter_depth)")

    if not len(input.shape) == 3:
        raise ValueError("The size of the input should be (input_height, input_width, input_depth)")

    if not filter.shape[2] == input.shape[2]:
        raise ValueError("the input and the filter should have the same depth.")

    input_w, input_h = input.shape[1], input.shape[0]
    filter_w, filter_h = filter.shape[1], filter.shape[0]

    if padding == 'VALID':
        output_h = int(ceil(float(input_h - filter_h + 1) / float(strides[0])))
        output_w = int(ceil(float(input_w - filter_w + 1) / float(strides[1])))

        output = np.zeros((output_h, output_w))  # convolution output

        for x in range(output_w):  # Loop over every pixel of the output
            for y in range(output_h):
                # element-wise multiplication of the filter and the image
                output[y, x] = (filter * input[y * strides[0]:y * strides[0] + filter_h,
                                         x * strides[1]:x * strides[1] + filter_w, :]).sum() + bias

    if padding == 'SAME':
        output_h = int(ceil(float(input_h) / float(strides[0])))
        output_w = int(ceil(float(input_w) / float(strides[1])))

        if input_h % strides[0] == 0:
            pad_along_height = max((filter_h - strides[0]), 0)
        else:
            pad_along_height = max(filter_h - (input_h % strides[0]), 0)
        if input_w % strides[1] == 0:
            pad_along_width = max((filter_w - strides[1]), 0)
        else:
            pad_along_width = max(filter_w - (input_w % strides[1]), 0)


        pad_top = pad_along_height // 2 #amount of zero padding on the top
        pad_bottom = pad_along_height - pad_top # amount of zero padding on the bottom
        pad_left = pad_along_width // 2             # amount of zero padding on the left
        pad_right = pad_along_width - pad_left      # amount of zero padding on the right

        output = np.zeros((output_h, output_w, input.shape[2]))  # convolution output
        image_padded = np.zeros((input.shape[0] + pad_along_height, input.shape[1] + pad_along_width, input.shape[2]))
        image_padded[pad_top:-pad_bottom, pad_left:-pad_right, :] = input

        for x in range(output_w):  # Loop over every pixel of the output
            for y in range(output_h):
                for z in range(input.shape[2]):
                # element-wise multiplication of the filter and the image

                    output[y, x, z] = np.multiply(filter[:,:,z],image_padded[y * strides[0]:y * strides[0] + filter_h, x * strides[1]:x * strides[1] + filter_w, z]).sum()

    return output


def create_gaussian_kernel(radius, sigma=10):
    """
    Creates a 2-dimensional, size x size gaussian kernel.
    It is normalized such that the sum over all values = 1.
    Args:
        size (int):     The dimensionality of the kernel. It should be odd.
        sigma (float):  The sigma value to use
    Returns:
        A size x size floating point ndarray whose values are sampled from the multivariate gaussian.
    See:
        https://en.wikipedia.org/wiki/Multivariate_normal_distribution
        https://homepages.inf.ed.ac.uk/rbf/HIPR2/eqns/eqngaus2.gif
    """

    # Ensure the parameter passed is odd
    size = 2*radius+1
    # TODO: Create a size by size ndarray of type float32
    x, y = np.meshgrid(np.linspace(-radius, radius, size, dtype=np.float32), np.linspace(-radius, radius, size, dtype=np.float32))
    # TODO: Populate the values of the kernel. Note that the middle `pixel` should be x = 0 and y = 0.
    rv = (np.exp(-(x ** 2 + y ** 2) / (2.0 * sigma ** 2))) / (2.0 * np.pi * sigma ** 2)
    # TODO:  Normalize the values such that the sum of the kernel = 1
    rv = rv / np.sum(rv)
    #rv = np.broadcast_to(rv[..., None], rv.shape + (3,))
    rv = np.repeat(rv[:, :, np.newaxis], 3, axis=2)
    return rv

filter = create_gaussian_kernel(1)
print(filter.shape)
#
output = convolutiond2d(miyoshi,filter)
data = np.array(output*255, dtype=np.uint8)
img = Image.fromarray(data)
img.show()



# a = np.array([[1, 2], [1, 2]])
# print(a)
# # (2,  2)
#
# # indexing with np.newaxis inserts a new 3rd dimension, which we then repeat the
# # array along, (you can achieve the same effect by indexing with None, see below)
# b = np.repeat(a[:, :, np.newaxis], 3, axis=2)
# print(b[:, :, 0])

