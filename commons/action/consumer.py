#-*- encoding: utf-8 -*-
'''
consumer.py
Created on 20-10-16 下午3:13
Copyright (c) 20-10-16, 海牛学院版权所有.
@author: 潘牛
'''
import threading,time,random,sys
sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from Util.log_util import LogUtil
from commons.action.base_consumer_action import ConsumerAction

class Consumer(threading.Thread):
    '''
    定义消费线程类
    '''

    def __init__(self, queue, thread_name, max_sleep_time, max_retry_num):
        '''

        :param queue:           Queue对象
        :param thread_name:     线程名称，用于写日志用
        :param max_sleep_time:  消费一次的最大的休眠间隔时间
        :param max_retry_num:   消费失败的最大重试次数
        '''

        # 调用父类的__init__() 初始化
        super(self.__class__, self).__init__()

        # 初始化属性
        self.queue = queue
        self.thread_name = thread_name
        self.max_sleep_time = max_sleep_time
        self.max_retry_num = max_retry_num


        # 初始化日志对象
        self.logger = LogUtil().get_logger(thread_name, thread_name)

    def run(self):
        self.logger.info("%s thread running..." % self.thread_name)
        while True:
            try:

                # 获取随机休眠时间
                random_sleep_time = round(random.uniform(0.2, self.max_sleep_time), 2)

                # 1) 从队列里取，标记完成
                c_action = self.queue.get()
                self.queue.task_done()

                # 校验 c_action 有效性
                if not isinstance(c_action, ConsumerAction):
                    raise Exception("%s is not ConsumerAction instance" % c_action)
                # 把处理该c_action对象的线程名称赋给 c_action.current_thread_name
                c_action.current_thread_name = self.thread_name

                # 获取开始时间
                start_time = time.time()

                # 2）调用action 消费
                result = c_action.action()

                # 获取结束时间
                end_time = time.time()

                run_time = end_time - start_time
                # 成功失败标记
                is_success = result[0]

                # 打印日志
                self.logger.info("thread.name=【%s】, run_time=%.2f s, sleep_time=%.2f s,"
                                 " retry_times=%d, result=%s, detail=%s" %
                                 (self.thread_name,
                                  run_time,
                                  random_sleep_time,
                                  c_action.current_retry_num + 1,
                                  "SUCCESS" if is_success else "FAIL",
                                  result[1:] if len(result) > 1 else "NONE"))

                # 3）如果消费失败会执行重试，有重试次数限制
                # 重试的契机：当前的重试次数 已经达到最大重试次数
                if not is_success and c_action.current_retry_num < self.max_retry_num - 1:
                    # 重试：当前重试次数+1 ，把c_action 还回队列
                    c_action.current_retry_num += 1
                    self.queue.put(c_action)

                # 4）休眠
                time.sleep(random_sleep_time)

            except Exception,e:
                self.logger.exception(e)