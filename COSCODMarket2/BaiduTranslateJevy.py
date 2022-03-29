# !/usr/bin/env python
# -*-coding:utf-8 -*-
"""
# File       : BaiduTranslateJevy.py
# Time       ：2022/1/20 15:22
# Author     ：jieWei_yang
# version    ：python 3.7.9
# Description：
"""





import requests
import random
import json
from hashlib import md5


# Set your own appid/appkey.
from icecream import ic

appid = '20200918000568551'
appkey = 'UPZWCVZrsAfn0y5wUMQc'

# For list of language codes, please refer to `https://api.fanyi.baidu.com/doc/21`
from_lang = 'en' # 要翻译的内容
to_lang =  'zh'  # 翻译成为中文

# Generate salt and sign
def make_md5(s, encoding='utf-8'):
    return md5(s.encode(encoding)).hexdigest()

def baiDuFanYi(query):
    # Build request
    query=query.replace('\n',' ')  # 将换行字符 去掉
    salt = random.randint(32768, 65536)
    sign = make_md5(appid + query + str(salt) + appkey)

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    payload = {'appid': appid, 'q': query, 'from': from_lang, 'to': to_lang, 'salt': salt, 'sign': sign}

    # Send request
    url='http://api.fanyi.baidu.com/api/trans/vip/translate'
    r = requests.post(url, params=payload, headers=headers)
    result = r.json() # dict
    result_str = json.dumps(result, indent=4, ensure_ascii=False) # str
    ic(result_str)
    # 得到的结果正常则 return
    if 'trans_result' in result_str:
        dst = result['trans_result'][0]['dst']
        return dst

if __name__ == '__main__':
    sentence='toilet paper'
    dst = baiDuFanYi(sentence)
