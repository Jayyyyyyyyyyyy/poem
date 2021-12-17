# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2020/3/17 11:41 AM
# @File      : batch_normalization_layer.py
# @Software  : PyCharm
# @Company   : Xiao Tang

# https://kratzert.github.io/2016/02/12/understanding-the-gradient-flow-through-the-batch-normalization-layer.html batch norm
import numpy as np


def batchnorm_forword(x, gamma, beta, eps):
    N, D = x.shape
    #step1: calculate mean
    mu = 1./N * np.sum(x, axis=0)
    # step2: subtract mean vector of every trainings example
    xmu = x - mu

    # step3: following the lower branch - calculation denominator
    sq = xmu ** 2

    # step4: calculate variance
    var = 1. / N * np.sum(sq, axis=0)

    # step5: add eps for numerical stability, then sqrt
    sqrtvar = np.sqrt(var + eps)

    # step6: invert sqrtwar
    ivar = 1. / sqrtvar

    # step7: execute normalization
    xhat = xmu * ivar

    # step8: Nor the two transformation steps
    gammax = gamma * xhat

    # step9
    out = gammax + beta

    # store intermediate
    cache = (xhat, gamma, xmu, ivar, sqrtvar, var, eps)

    return out, cache