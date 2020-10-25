#-*- encoding: utf-8 -*-
'''
producer.py
Created on 20-10-16 下午2:45
Copyright (c) 20-10-16, 海牛学院版权所有.
@author: 潘牛
'''
import threading,time,sys
sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from Util.log_util import LogUtil
from commons.action.consumer import Consumer
from commons.action.base_producer_action import ProducerAction
class Producer(threading.Thread):
    '''
    定义生产线程类
    '''

    def __init__(self, queue, queue_name, p_action, p_sleep_time,
                 c_max_num, c_max_sleep_time, c_max_retry_num):
        """

        :param queue:            Queue对象，用于往队列里放数据
        :param queue_name:       每个业务，都有自己的队列名称，用于生成业务的生产、消费线程名称
        :param p_action:         生产线程用生产动作对象来创建对象实例列表
        :param p_sleep_time:     生产线程生产一次后，下次再生产的休眠间隔时间
        :param c_max_num:        消费线程数
        :param c_max_sleep_time: 消费线程消费一次最大休眠间隔时间
        :param c_max_retry_num:  消费线程最大消费重试次数（重试次数的设定）
        :return:
        """

        # 调用父类的__init__() 初始化
        super(self.__class__, self).__init__()

        # 初始化属性
        self.queue = queue
        self.queue_name = queue_name
        self.p_action = p_action
        self.p_sleep_time = p_sleep_time
        self.c_max_num = c_max_num
        self.c_max_sleep_time = c_max_sleep_time
        self.c_max_retry_num = c_max_retry_num


        # 校验 p_action 有效性
        if not isinstance(self.p_action, ProducerAction):
            raise Exception("%s is not ProducerAction instance" % self.p_action)

        # 初始化logger对象
        self.logger = LogUtil().get_logger("%s_producer" % self.queue_name, "%s_producer" % self.queue_name)


    def run(self):
        '''
        线程体
        '''
        self.logger.info('%s_producer thread running...' % self.queue_name)

        c_actions = []
        while True:
            try:

                # 获取开始时间
                start_time = time.time()

                # 1) 生产ConsumerAction对象实例列表
                if len(c_actions) == 0:
                    c_actions = self.p_action.queue_items()

                total_num = len(c_actions)
                self.logger.info("thread.name=【%s_producer】, current time produce %d actions"
                                 % (self.queue_name, total_num))

                # 2) 一个一个往队列里放
                while True:
                    if len(c_actions) == 0:
                        break

                    # 如果满足往队列里放的契机，就往队列里放；否则等待
                    # 契机：队列的未完成数 <= 消费者线程数
                    if self.queue.unfinished_tasks <= self.c_max_num:
                        c_action = c_actions.pop()
                        self.queue.put(c_action)

                # 获取结束时间
                end_time = time.time()

                # 运行时长是秒
                run_time = end_time - start_time
                if int(run_time) == 0:
                    rate = int(60 * float(total_num) / 0.01)
                else:
                    rate = int(60 * float(total_num) / run_time)


                # 记录生成速率
                self.logger.info("thread.name=【%s_producer】, total_num=%d, produce %d actions/min, "
                                 "sleep_time=%d" % (self.queue_name, total_num, rate, self.p_sleep_time))


                # 3) 休眠
                time.sleep(self.p_sleep_time)

            except Exception,e:
                self.logger.exception(e)



    def start_work(self):
        # 启动生产线程
        self.logger.info("%s_producer thread start" % self.queue_name)
        self.start()

        # 根据消费线程数，创建并启动消费线程
        for i in range(1, self.c_max_num + 1):
            c_thread_name = "%s_consumer_%d_!" % (self.queue_name, i)
            self.logger.info("%s thread create and start" % c_thread_name)
            c = Consumer(self.queue, c_thread_name, self.c_max_sleep_time, self.c_max_retry_num)
            c.start()


if __name__ == '__main__':
    produder = Producer()
    # 启动线程
    produder.start()