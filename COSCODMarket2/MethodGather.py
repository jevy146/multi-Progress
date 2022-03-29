# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : MethodGather.py
# Time       ：2022/1/19 15:33
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""


# 计算 排名 单调性

import pandas  as pd
from icecream import ic

from ClientMongo import OperateMongo


def get_continue_seq(rank_list):
    ls = rank_list
    len_ls = len(ls)
    index_count = {}
    for x in range(len_ls):
        key = index_count.keys()
        if key:
            diff_x1 = ls[x] - ls[x - 1]
            if diff_x1 < 0:  # 减区间
                index_count[max(key)].append(ls[x])
            else:
                index_count[x] = [ls[x]]
        else:
            index_count[x] = [ls[x]]
    res = index_count[max(index_count, key=lambda x: len(index_count[x]))]
    # print('index_count', index_count)

    return len(res)  # 最大减区间

# 计算斜率的单调性
def get_slope_seq(rank_list):
    ls = rank_list
    len_ls = len(ls)
    index_count = {}
    for x in range(len_ls):
        key = index_count.keys()

        if key:
            if ls[x] == '数据缺失':
                break  # 这里有一个规律，按照给定的顺序，最先出现“数据缺失”，后面的均是
            if ls[x] <= 0  and  ls[x - 1] < 0:
                index_count[max(key)].append(ls[x])
            else:
                index_count[x] = [ls[x]]
        else:
            index_count[x] = [ls[x]]
    res = index_count[max(index_count, key=lambda x: len(index_count[x]))]

    return len(res)


# 定义一个函数 求对应的斜率
# 求斜率
import numpy as np
def trendline(data): # 时间序列的数据
    order=1
    index=[i for i in range(1,len(data)+1)]
    coeffs = np.polyfit(index, list(data), order)  #
    slope = coeffs[-2]   # array([ 1.10714286, -0.57142857])
    return round(float(slope),2)


# 判断求斜率的所需要的时间断
# 这里有连个问题，
#1. 如果缺失了数据没有进行处理，直接求会影响到slope的大小
def judge_data_complete(rank_data, aba_word, sort_date):
    rank_num = [rank_data[week] if week in aba_word['date_day'].tolist() else None for week in sort_date]
    if None in rank_num:
        return '数据缺失'
    else:
        return trendline(rank_num)




# 转化占比 异常值 处理
def click_and_con(interval, aba_word, sort_date):
    num_aba = aba_word[aba_word['date_day'].isin(sort_date)].iloc[:, 2:-1]
    mean_queue = num_aba.mean().apply(lambda x: round(x, 2))
    out_1 = {key + '_' + interval: val for key, val in dict(mean_queue).items()}
    return out_1


 # 传入一个时间序列 这里需要计算关键词缺少的次数
def count_lack_date(start_date:str,lego_date_original:list):
    lego_date=[parse(each).strftime('%Y-%m-%d') for each in  lego_date_original]
    date_seat=lego_date.index(start_date)
    length_seat=len(lego_date[date_seat:])
    return length_seat


# 获取关键词的排名分布
def run_des(rankSeries ):
    ''' 下面 对平稳性进行检验'''
    rank_desc = dict(rankSeries.describe())  # 会产生数据的个数
    rank_desc['min_index'] = rankSeries.idxmin().strftime('%Y-%m-%d')
    rank_desc['start'] = rankSeries.index[0].strftime('%Y-%m-%d')  # 起始时间
    return rank_desc

from dateutil.parser import parse
# 处理 列表 中 dataFrame 的格式
def deal_list_df(res_list:list):
    dt_de = pd.concat(res_list)
    if dt_de.empty:
        return dt_de
    else:
        dt_demo = dt_de.drop_duplicates(subset=['date_day'], keep='first', inplace=False)  # 删除重复项
        dt_demo.index = dt_demo["date_day"].map(lambda x: parse(x))
        dt_sort = dt_demo.sort_index()
        return dt_sort


# 计算 slope值 和对应的指标 list_sort_date 为lego 的日期格式
def main_slope(aba_word, lego_sort_date):
    ''' 1. 获取所需要的计算的日期  '''
    ''' 对应 输出的格式  周计算 '''
    week_calculate = {
        'week_three': lego_sort_date[-3:],  # 近3周
        'one_month': lego_sort_date[-4:],  # 近1个月
        'two_month': lego_sort_date[-8:],  # 近2个月
        'three_month': lego_sort_date[-13:],  # 近3个月
        'six_month': lego_sort_date[-26:],  # 近6个月
        'nine_month': lego_sort_date[-39:],  # 近9个月
        'twelve_month': lego_sort_date[-52:],  # 近12个月
    }

    rank_data = dict(zip(aba_word['date_day'], aba_word['rank']))
    # ic(rank_data)  # 全部的数据 日期 : 排名
    # 1 按照对应的周数据 求对应的斜率 并 判断 数据缺失情况，有的个别周数据不全，就不求斜率了 依次 全部的数据//lego对应的日期范围
    out_info_1 = {key + '_slope': judge_data_complete(rank_data, aba_word, val) for key, val in week_calculate.items()}


    # 3. 计算 斜率 的单调性 。。
    seq = ['week_three_slope', 'one_month_slope', 'two_month_slope', 'three_month_slope', 'six_month_slope',
           'nine_month_slope', 'twelve_month_slope']  # 固定顺序
    slope_seq = [out_info_1[ky] for ky in seq]

    try: #上周排名
        last_week_rank = rank_data[lego_sort_date[-2]]
    except:
        last_week_rank = None
    # 4 结果合集
    out_info_3 = {
        'this_week': lego_sort_date[-1],
        'this_week_rank': rank_data[lego_sort_date[-1]],
        'last_week_rank': last_week_rank,  # 有可能第一次出现这个词 lanyard kdracoip
        'rank_seq': get_continue_seq(aba_word['rank'].tolist()),  # 计算 排名 单调区间长度    ''' 将关键词的 连续变化区间 加上   '''
        'slope_seq': get_slope_seq(slope_seq),  # 计算 斜率 连续为 负数 的次数
    }

    out_info_3['start_date'] = aba_word['rank'].index[0].strftime('%Y-%m-%d')  # 起始时间
    out_info_3['lack_count'] = count_lack_date(out_info_3['start_date'],lego_sort_date)- len(aba_word)  # 计算 缺失个数
    dict_info_all = {**out_info_3, **out_info_1, }
    return  dict_info_all


# 点击/转化集中度
def click_con_sum(this_week_row):
    #  # 点击集中度 点击，关键词都会生成点击
    click_focus = this_week_row['click_1'] +this_week_row['click_2']+this_week_row['click_3']
    if this_week_row['Conversion_1'][0] == "—":  # 索引均为0
        con_focus = "—"  # 这个代表无单
    else:
        con_focus = this_week_row['Conversion_1'] + this_week_row['Conversion_2'] + this_week_row['Conversion_3']
        con_focus = round(list(con_focus)[0], 2)

    return {'this_click_focus': int(round(list(click_focus)[0], 2)*100) ,
         'this_con_focus': int(con_focus*100)   }


# 1  获取  LEGO 的周数据
from  ClientMongo import OperateMongo
from  MongoDataBases  import  week_aba_years
def lego_date_week(db_name):
    res_lego =[OperateMongo(db_name, table_name).lego_date() for table_name in week_aba_years]
    res_sort=deal_list_df(res_lego)
    lego_date=res_sort['date_day'].tolist()
    return lego_date