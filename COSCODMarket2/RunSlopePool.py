# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : RunSlopePool.py
# Time       ：2022/1/19 14:49
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""
import os
import time

from  ClientMongo import OperateMongo
from KeyWordTag import key_word_tag
from MongoDataBases import week_aba_years, db_names
from icecream import ic
from MethodGather import deal_list_df, lego_date_week,   main_slope, click_con_sum

import multiprocessing
from multiprocessing import Process
from multiprocessing import Pool



def pool1(db_name,keyWord,lego_date_list):
    print('Run task %s (%s)...' % ('pool1', os.getpid()))
    # 1 没有日期的时候 仅仅获取 排名的时间序列  计算斜率
    res_words_week = [OperateMongo(db_name, table_name).aba_rank_week(seed_word=keyWord) for table_name in
                      week_aba_years]
    aba_word_week = deal_list_df(res_words_week)
    info3 = main_slope(aba_word_week, lego_date_list)
    # 5 增加排名最小的时间 月份数据并不及时 数据标签
    info3['min_rank_date'] = aba_word_week['rank'].idxmin().strftime('%Y-%m-%d')  # 全部时间 的排名数据
    info3['searchTerm'] = keyWord
    info3['wordsLength'] = len(keyWord.split(' '))  # 关键词长度
    # 3 获取 关键词的 类目 kinds_2021_aba  # 按照月份下载的  date_day='2021-12-31'
    res_words_kinds = OperateMongo(db_name, kinds_table).aba_kinds(seed_word=keyWord, date_day=kinds_date_day)
    # Empty DataFrame / None
    if res_words_kinds:
        info3['kinds'] = res_words_kinds['country']
    else:
        info3['kinds'] = 'others'
    return info3


def pool2(db_name,keyWord,lego_date_list):
    print('Run task %s (%s)...' % ('pool2', os.getpid()))
    # 2 最新日期  获取最新的 点击/转化 集中度
    date_day = lego_date_list[-1]
    res_words_day = OperateMongo(db_name, f'week_{date_day[:4]}_aba').aba_rank_week(seed_word=keyWord,
                                                                                    seed_week=date_day)
    #ic(res_words_day)
    focus = click_con_sum(res_words_day)  # 点击/转化集中度
    return focus
def pool3(db_name,keyWord):
    print('Run task %s (%s)...' % ('pool3', os.getpid()))
    # 4 使用月份的数据 进行 傅立叶 求周期
    tag_wd=key_word_tag(db_name,keyWord)
    return tag_wd


def pool_process_main(db_name,keyWord,lego_date_list):
    pool = Pool(processes=3)
    info3 = pool.apply_async(pool1, args=(db_name,keyWord,lego_date_list))
    focus = pool.apply_async(pool2, args=(db_name,keyWord,lego_date_list))
    tag_wd = pool.apply_async(pool3, args=(db_name,keyWord ))
    #print('Main Process started')
    pool.close()
    pool.join()
    #print('Main Process ended')
    result = {**info3.get(), **focus.get(), **tag_wd.get()}
    ic(result)
    return result

kinds_table='kinds_2021_aba' # 查找类目的数据库
kinds_date_day='2022-01-31' # 最新类目ABA数据日期
slope_table='slope_2022_market' #  slope 选品的数据库 计算的结果写入
last_week_aba_table=week_aba_years[-1] #

''' 版本描述： 使用多进程  每次都是重新打开新的进程 消耗算力  运行速度3'''

if __name__ == '__main__':

    #全部封装好，自动化运行
    for db_name in db_names[1:2]:
        ic(db_name)
        lego_date_list = lego_date_week(db_name)  # 获取全部 日期 序列
        ic(lego_date_list[-1])
        # 1. 第一部 获取 最新日期的 的 top 关键词   2022年的数据 最近一周的数据
        top_words=OperateMongo(db_name,last_week_aba_table).get_top_search_term(lego_date_list[-1])
        start = time.time()
        for ky in top_words[300:400]:
            ic(ky)
            # 2. 将关键词计算指标保存 至 数据库
            result=pool_process_main(db_name, ky, lego_date_list)
            # 3. 保存数据
            OperateMongo(db_name, 'slope_2022_market').slope_insert_one(result)
        end = time.time()
        ic(  end-start )
