# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/19 3:41 PM
# @File      : insert_sort.py
# @Software  : PyCharm
# @Company   : Xiao Tang

import time
import random
def insert_sort(A):
    for i in range(len(A)-1):
        j = i+1
        key = A[j]
        while j>=1 and key < A[j-1]:
            A[j] = A[j-1]
            j -= 1
        A[j] = key
    return A


def insert_sort2(A):
    for i in range(len(A)-1):
        j = i+1
        key = A[j]
        while j>=1 and key < A[j-1]:
            A[j-1], A[j] = A[j], A[j-1]
            j -= 1
    return A

tmp =  [random.randint(1,12000) for x in range(12000) ]
tmp1= tmp.copy()
print("insert_sort2")
print(time.ctime())
insert_sort2(tmp)
print(time.ctime())
print("insert_sort")
print(time.ctime())
insert_sort(tmp1)
print(time.ctime())
#insert_sort > insert_sort2