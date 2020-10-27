#-*- encoding: utf-8 -*-
'''
config.py
Created on 2019/1/4 9:29
Copyright (c) 2019/1/4, 海牛学院版权所有.
@author: 潘牛
'''

#日志地址
#windows环境下的测试目录
_LOG_DIR = '/tmp/python/log/%s'
#linux集群下的正式目录
# _LOG_DIR = '/home/wheeldestiny26/python/hainiu_crawler/log/%s'

#数据地址
#windows环境下的测试目录
_LOCAL_DATA_DIR = '/tmp/python/data/%s'
#linux集群下的正式目录
# _LOCAL_DATA_DIR = '/home/wheeldestiny26/python/hainiu_crawler/data/%s'

#数据库配置_测试
_HAINIU_DB = {'HOST':'nn2.hadoop', 'USER':'hainiu', 'PASSWD':'12345678', 'DB':'hainiutest', 'CHARSET':'utf8', 'PORT':3306}

# NAME, P_SLEEP_TIME, C_MAX_NUM, C_MAX_SLEEP_TIME, C_RETRY_TIMES
_QUEUE_DEMO = {'NAME':'demo', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}


_QUEUE_APPLE = {'NAME':'apple', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}


_QUEUE_HAINIU = {'NAME':'hainiu', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 1,
                 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                 'LIMIT_NUM': 2, "PAGE_SHOW_NUM": 5}
# 生产新闻的参数配置
_QUEUE_PRODUCE_NEWS = {'NAME':'produceNews', 'P_SLEEP_TIME': 3, 'C_MAX_NUM': 3,
                    'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3, 'MAX_FAIL_TIMES': 6,
                    'LIMIT_NUM': 2, "PAGE_SHOW_NUM": 5}
#报警电话
_ALERT_PHONE = '110'

