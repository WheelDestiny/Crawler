#-*- encoding: utf-8 -*-
'''
base_producer_action.py
Created on 20-10-16 下午2:08
Copyright (c) 20-10-16, 海牛学院版权所有.
@author: 潘牛
'''

class ProducerAction(object):
    '''
    生产动作基类，用于制定生产规则
    '''

    def queue_items(self):
        '''
        该方法是空方法，需要子类实例继承该方法，实现生产具体业务的 ConsumerAction对象实例列表
        :return:
        '''
        pass