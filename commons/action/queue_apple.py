#-*- encoding: utf-8 -*-
'''
queue_apple.py
Created on 20-10-16 下午2:12
Copyright (c) 20-10-16, 海牛学院版权所有.
@author: 潘牛
'''
import Queue,time,traceback

from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from commons.action.producer import Producer
from Config.config import _QUEUE_APPLE

class AppleProducerAction(ProducerAction):
    '''
    定义苹果的生产动作类，用于生产AppleConsumerAction对象列表
    '''

    def queue_items(self):
        c_actions = []
        for i in range(1, 11):
            print '生产的苹果：apple_%d' % i
            c_action = AppleConsumerAction("apple_%d" % i)
            c_actions.append(c_action)
        return c_actions


class AppleConsumerAction(ConsumerAction):
    '''
    定义苹果的消费动作类，用实现苹果的消费逻辑
    '''

    def __init__(self, id):
        # 主动调用父类，加载公共属性
        super(self.__class__,self).__init__()

        self.id = id

    def action(self):
        print '消费的苹果：%s' % self.id
        # time.sleep(2)
        # TODO 调试修改
        # return [False, self.id]
        # return [True, self.id]
        return self.result(True,self.id,1,2,3,4,5)

    def success_action(self,values):
        print '成功后动作：%s' % values[0]
        print '值：%d' % values[1]

    def fail_action(self, values):
        print '失败后动作：%s' % values[0]

    # def action(self):
    #     is_success = True
    #     try:
    #         print '执行业务'
    #     except Exception,e:
    #         is_success = False
    #         traceback.print_exc(e)
    #     if is_success:
    #         print '成功后的逻辑'
    #     else:
    #         print '失败后的逻辑'

if __name__ == '__main__':

    # ------多线程逻辑--------
    # 创建AppleProducerAction
    p_action = AppleProducerAction()

    # 创建Queue对象
    queue = Queue.Queue()

    # {'NAME':'apple', 'P_SLEEP_TIME': 5, 'C_MAX_NUM': 1, 'C_MAX_SLEEP_TIME': 3, 'C_RETRY_TIMES':3}
    # 创建Producer线程对象
    producer = Producer(queue,
                        _QUEUE_APPLE['NAME'],
                        p_action,
                        _QUEUE_APPLE['P_SLEEP_TIME'],
                        _QUEUE_APPLE['C_MAX_NUM'],
                        _QUEUE_APPLE['C_MAX_SLEEP_TIME'],
                        _QUEUE_APPLE['C_RETRY_TIMES']
                        )

    # 启动生产线程 + 创建和启动消费线程
    producer.start_work()

    #-------单线程逻辑-------
    # # 创建AppleProducerAction
    # p_action = AppleProducerAction()
    # # 生产
    # c_actions = p_action.queue_items()
    #
    # # 创建Queue对象
    # queue = Queue.Queue()
    #
    # # 往队列里放
    # while True:
    #     if len(c_actions) == 0:
    #         break
    #     c_action = c_actions.pop()
    #     queue.put(c_action)
    #
    #
    # # 从队列里取出并消费
    # while True:
    #     if queue.empty():
    #         break
    #     c_action = queue.get()
    #     c_action.action()