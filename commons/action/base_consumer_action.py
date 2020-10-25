#-*- encoding: utf-8 -*-
'''
base_consumer_action.py
Created on 20-10-16 下午2:10
Copyright (c) 20-10-16, 海牛学院版权所有.
@author: 潘牛
'''
class ConsumerAction(object):
    '''
    消费动作基类，用于制定消费规则
    '''

    def __init__(self):
        # 初始化当前重试次数
        self.current_retry_num = 0
        # 初始处理当前对象线程的名称
        self.current_thread_name = ""

    def action(self):
        '''
        该方法时空方法，需要子类实例实现具体业务的消费逻辑
        :return:
        '''
        pass

    def result(self,is_success, *values):
        '''
        根据成功失败，来执行成功后的逻辑或失败后的逻辑
        并返回 列表 格式：[is_success, 1,2,3,4,5]
        :param is_success:
        :param values:
        :return:
        '''
        if is_success:
            self.success_action(values)
        else:
            self.fail_action(values)
        result_list = [is_success]
        for v in values:
            result_list.append(v)

        return result_list


    def success_action(self,values):
        '''
        成功后的动作，这个是空方法，需要子类实现具体的成功后业务逻辑
        :param values: 元组
        :return:
        '''
        pass

    def fail_action(self, values):
        '''
        失败后的动作，这个是空方法，需要子类实现具体的失败后业务逻辑
        :param values: 元组
        :return:
        '''
        pass