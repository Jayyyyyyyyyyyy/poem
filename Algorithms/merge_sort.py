# -*- coding: utf-8 -*-
# @Author    : Chenxi Jiang
# @Time      : 2019/9/20 8:58 PM
# @File      : merge_sort.py
# @Software  : PyCharm
# @Company   : Xiao Tang
import time
import random
def merge(A,p,q,r):
    print("*******")
    print(p,q,r)
    n1  = q-p+1
    n2 = r-q
    L = [999]*(n1+1)
    R = [999]*(n2+1)
    for i in range(n1):
        L[i] = A[p+i-1]
    for j in range(n2):
        R[j] = A[q+j]
    print(L)
    print(R)
    print("***")
    i = 0
    j = 0
    for x in range(p,r):
        if L[i] <= R[j]:
            A[x] = L[i]
            i+=1
        else:
            A[x] = R[j]
            j+=1
    print(A)
def merge_sort(A,p,r):
    if p<r:
        print(p,r)
        q = int((p+r)/2)
        merge_sort(A,p,q)
        merge_sort(A,q+1,r)
        merge(A,p,q,r)






tmp =  [random.randint(1,10) for x in range(10) ]
print(tmp)
print("merge_sort")
print(time.ctime())
merge_sort(tmp,0,len(tmp)-1)
print(time.ctime())
print(tmp)
#insert_sort > insert_sort2