# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : ClientMongo.py
# Time       ：2022/1/19 14:04
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""


import pymongo
import pandas as pd


class OperateMongo(object):

    def __init__(self, db_name, table_name):
        client = pymongo.MongoClient(host="192.168.9.108", port=27017)
        self.db = client[db_name]
        self.collection = self.db[table_name]  # 数据库的名称

    def lego_date(self):
        res = self.collection.find({'words': 'lego', },
                                   {"_id": 0,  'date_day': 1, })
        df_lego=pd.DataFrame(res)
        return df_lego

    # 这里获取 全部 应该有的 周的数据，
    def aba_rank_week(self,seed_word=None , seed_week=None):
        if seed_week:
            res=self.collection.find({'words': seed_word, 'date_day': seed_week},
                                     {"_id": 0, 'rank': 1, 'date_day': 1, 'words': 1,
                                      'click_1': 1, 'Conversion_1': 1, 'click_2': 1, 'Conversion_2': 1, 'click_3': 1,
                                      'Conversion_3': 1}).hint([('words',1),('date_day', -1)]) # 隐藏技能，索引的顺序严格一致
            df_demo = pd.DataFrame(res)

        else: # 没有日期的时候 只有 排名 和日期
            res = self.collection.find({'words': seed_word,  },
                                       {"_id": 0, 'rank': 1, 'date_day': 1,})
            df_demo=pd.DataFrame(res)

        return  df_demo

    def aba_rank_month(self,seed_word=None  ):

        res = self.collection.find({'words': seed_word,  },
                                   {"_id": 0, 'rank': 1, 'date_day': 1})
        df_demo=pd.DataFrame(res)

        return  df_demo

    def aba_kinds (self,seed_word=None ,date_day=None ):

        res = self.collection.find_one({'words': seed_word, 'date_day':date_day },
                                   {"_id": 0, 'rank': 1, 'date_day': 1 ,'country': 1})

        return  res

    # 获取 前 4 千 的关键词 小于等于 '$lte'
    def get_top_search_term(self, last_day):

        # 获取前 5 万个 关键词
        res = self.collection.find({
            'date_day': last_day,
            'rank': {'$lte': 50000},
        },
            {
                '_id': 0,
                'words': 1,
                'rank': 1,
            }
        )
        df_mongo = pd.DataFrame(res).sort_values('rank')
        searchWords = df_mongo['words'].tolist()
        return searchWords

    def slope_insert_one(self, dict_data):
        self.collection.insert_one(dict_data)
        return




if __name__ == '__main__':
    db_names = ['CA_AMZ', 'DE_AMZ', 'ES_AMZ', 'FR_AMZ', 'IT_AMZ', 'JP_AMZ', 'UK_AMZ', 'US_AMZ']
    week_aba_years = ['week_2019_aba', 'week_2020_aba', 'week_2021_aba']
    week_aba_month = ['month_2020_aba', 'month_2021_aba']



