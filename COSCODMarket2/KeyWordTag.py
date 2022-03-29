# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : KeyWordTag.py
# Time       ：2022/1/20 15:03
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""
import os

from  ClientMongo import OperateMongo
from MongoDataBases import  week_aba_months
from MethodGather import deal_list_df
from MonthPeriod import FourierPeriod

''' 针对关键词的 标签数据，新建一个表用来存放
使用弹出窗口的方式进行展示。
 关键词的指标 分为：周期性、关键词长度、翻译、 # 年度最大值、最小值、 这个用图像就可以看出
 这样做太复杂了，增加了百度翻译之后，速度上不来 。
 
 百度翻译的 费用不够了，暂时启用不了。
 
 '''

# 4 获取 月份的数据 并 计算周期性
def key_word_tag(db_name,keyWord):
    print('Run task %s (%s)...' % ('pool3', os.getpid()))
    # 4 使用月份的数据 进行 傅立叶 求周期 周数据
    res_words_month = [OperateMongo(db_name, table_name).aba_rank_month(seed_word=keyWord) for table_name in
                       week_aba_months]

    aba_word_month = deal_list_df(res_words_month)  # 月份 ABA 关键词 2020-2021
    if aba_word_month.empty or len(aba_word_month) < 24:
        return {'wordTag': 'lacking'}
    else:
        res_per = FourierPeriod(aba_word_month['rank'])
        if res_per != [] and res_per[0] in [2, 4, 6]:
            return {'wordTag': 'seasonal'}
        else:
            return {'wordTag': 'non-seasonal'}


if __name__ == '__main__':
    key_word_tag('US_AMZ','lego')


