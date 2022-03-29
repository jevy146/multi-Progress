# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : RunSlope.py
# Time       ：2022/1/19 14:49
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""
import time

from  ClientMongo import OperateMongo
from KeyWordTag import key_word_tag
from MongoDataBases import week_aba_years, db_names
from icecream import ic
from MethodGather import deal_list_df, lego_date_week,   main_slope, click_con_sum


''' 在原有的基础上 对周期性结果 进行了修正， 统一了指标  '''
def key_word_slope(db_name,keyWord,lego_date_list):
    # 1 没有日期的时候 仅仅获取 排名的时间序列  计算斜率
    res_words_week = [OperateMongo(db_name, table_name).aba_rank_week(seed_word=keyWord) for table_name in week_aba_years]
    aba_word_week = deal_list_df(res_words_week)
    info3=main_slope(aba_word_week,lego_date_list)

    # 2 最新日期  获取最新的 点击/转化 集中度
    date_day=lego_date_list[-1]
    res_words_day = OperateMongo(db_name, f'week_{date_day[:4]}_aba').aba_rank_week(seed_word=keyWord,seed_week=date_day)
    ic(res_words_day)
    focus=click_con_sum(res_words_day) # 点击/转化集中度

   # 3 获取 关键词的 类目 kinds_2021_aba  # 按照月份下载的  date_day='2021-12-31'
    res_words_kinds = OperateMongo(db_name,kinds_table ).aba_kinds(seed_word=keyWord, date_day=kinds_date_day )
     # Empty DataFrame / None
    if res_words_kinds :
        info3['kinds'] = res_words_kinds['country']
    else:
        info3['kinds'] = 'others'


    # 4 使用月份的数据 进行 傅立叶 求周期
    tag_wd=key_word_tag(db_name,keyWord)

    #5 增加排名最小的时间 月份数据并不及时 数据标签
    info3['min_rank_date'] = aba_word_week['rank'].idxmin().strftime('%Y-%m-%d')  # 全部时间 的排名数据
    info3['searchTerm'] =   keyWord
    info3['wordsLength'] =  len( keyWord.split(' ') )  # 关键词长度
    # 6 合并所有的指标字典
    result={**info3,**focus,**tag_wd}
    ic(result)
    return result


kinds_table='kinds_2021_aba' # 查找类目的数据库
kinds_date_day='2022-01-31' # 最新类目ABA数据日期
slope_table='slope_2022_market' #  slope 选品的数据库 计算的结果写入
last_week_aba_table=week_aba_years[-1] # "week_2022_aba"

''' 版本描述： 没有使用多进程  运行速度2'''
# playstation 5  this_week  '2022-01-15'
if __name__ == '__main__':


    # 全部封装好，自动化运行
    for db_name in db_names[1:2]:
        ic(db_name)
        lego_date_list = lego_date_week(db_name)  # 获取全部 日期 序列
        ic(lego_date_list[-1])
        # 1. 第一部 获取 最新日期的 的 top 关键词   2022年的数据 前 5个关键词
        top_words=OperateMongo(db_name,last_week_aba_table).get_top_search_term(lego_date_list[-1])
        start = time.time()
        for ky in top_words[400:5000]:
            ic(ky)
            result=key_word_slope(db_name,ky,lego_date_list)
            # 7 保存到  数据库 数据  'slope_2022_market' 数据库  使用新的数据库 运行的数据明显加快
            OperateMongo(db_name, 'slope_2022_market').slope_insert_one(result)
        end = time.time()
        ic(end - start)

