# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : MonthPeriod.py
# Time       ：2022/1/18 17:55
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""



import numpy as np
from icecream import ic
from scipy.fftpack import fft
# 标记 波峰
from scipy import signal
import statsmodels.tsa.api as smt

''' 文件说明 ： 求 关键词 月份ABA排名数据 的周期 '''

def FourierPeriod(word_rank): # 传入时间序列 的 series

    length= len(word_rank)

    y =  word_rank.values # np格式
    yf = abs(fft(y))  # 取绝对值
    yf1 = yf / length  # 归一化处理 fft 的值
    yf2 = yf1[range(int(length / 2))]  # 由于对称性，只取一半区间 对应的 fft 的值

    yfhalf = yf2  # 傅里叶变化之后的值  取 一半
    fwbest = yfhalf[signal.argrelextrema(yfhalf, np.greater)]  # 波峰 对应 的数值
    xwbest = signal.argrelextrema(yfhalf, np.greater)  # 这个是对应的 index 位置  波峰的位置

    # 归一化处理 fft 的值 升序排序
    key_value = dict(zip(xwbest[0], fwbest))
    fft_value_sort = sorted(key_value.items(), key=lambda kv: (kv[1], kv[0]))  # 字典 按照 values 进行排序。

    '''
    周期的范围 只有三种 取得 最有可能的三种周期情况 
    一个季度：4个月   24/4=6 
    半年：6个月  24/6=4 
    一年：12个月  24/12=2
     '''
    # 找到 可能的周期 只找到 最可能的 3 种 周期
    may_period = [item[0] for item in fft_value_sort[-3:][::-1] if
                  item[0] <= ( length / 4 + 1)]  # 顺序不能出错，检测前面最优可能的周期能不能通过

    if may_period:
        #ic(may_period)
        # 求 自相关 系数
        nlags = may_period[-1] + 1  # 自相关  不同 相位差的自相关系数
        acf = smt.stattools.acf(yf2, nlags=nlags, fft=False)  # 传入的是 傅里叶之后 绝对值 再去一半
        acf_best = signal.argrelextrema(acf, np.greater)
        judg_per = [per for per in may_period if per in acf_best[0]]
        return judg_per
    else:
        return ['无周期']



if __name__ == '__main__':
    pass

# 注意函数的使用 以及返回值