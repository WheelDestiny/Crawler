#-*- encoding: utf-8 -*-
'''
queue_hainiu.py
Created on 20-10-17 下午2:24
Copyright (c) 20-10-17, 海牛学院版权所有.
@author: 潘牛
'''
import sys, traceback,Queue
sys.path.append('/home/hadoop/hainiu_crawler')
from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from commons.action.producer import Producer
from Util.db_util import DBUtil
from Util.util import Util
from Config.config import _HAINIU_DB, _QUEUE_HAINIU

class HainiuProducerAction(ProducerAction):
    '''
    从hainiu_queue 查询指定的条数，封装成HainiuConsumerAction对象列表，并返回
    '''

    def queue_items(self):
        # select ... for update 悲观锁sql （占位符）
#         select_queue_sql = """
# select id, action, params
# from hainiu_queue
# where type=%s and is_work=0 and fail_times<%s  limit 0,%s for update;
#         """

        # 屏蔽失败ip的查询
        select_queue_sql = """
select id, action, params
from hainiu_queue
where type=%s and is_work=0 and fail_times<%s and fail_ip!=%s limit 0,%s for update;
        """

        # 更新is_work为1的SQL (采用SQL拼接)
        update_queue_sql = """
        update hainiu_queue set is_work=1 where id in (%s);
        """

        c_actions = []
        db_util = DBUtil(_HAINIU_DB)
        # 存字符串id的列表
        ids = []
        try:
            # 采用 select ... for update 来进行行锁
            # sql_params = [1, _QUEUE_HAINIU['MAX_FAIL_TIMES'], _QUEUE_HAINIU['LIMIT_NUM']]
            # 屏蔽ip的查询参数
            ip = Util().get_local_ip()
            sql_params = [1, _QUEUE_HAINIU['MAX_FAIL_TIMES'], ip, _QUEUE_HAINIU['LIMIT_NUM']]
            res1 = db_util.read_dict(select_queue_sql,sql_params)
            # ({},{})
            for row in res1:
                id = row['id']
                ids.append(str(id))
                act = row['action']
                params = row['params']
                c_action = HainiuConsumerAction(id, act, params)
                c_actions.append(c_action)
            # 更新is_work为1
            if len(c_actions) > 0:
                db_util.execute_no_commit(update_queue_sql % ','.join(ids))

            # 提交事务
            db_util.commit()
        except Exception,e:
            db_util.rollback()
            traceback.print_exc(e)
        finally:
            db_util.close()

        return c_actions

class HainiuConsumerAction(ConsumerAction):

    def __init__(self, id, act, params):
        super(self.__class__, self).__init__()

        self.id = id
        self.act = act
        self.params = params

    def action(self):
        print 'id:%d, action:%s, params:%s' % (self.id, self.act, self.params)
        print self.current_thread_name
        # TODO 调试成功失败逻辑
        return self.result(True, self.id, self.act, self.params)



    def success_action(self,values):
        '''
        将hainiu_queue表中消费成功的记录删除。
        '''
        delete_sql = "delete from hainiu_queue where id=%s"
        db_util = DBUtil(_HAINIU_DB)
        try:
            sql_params = [self.id]
            db_util.execute(delete_sql, sql_params)

        except Exception,e:
            traceback.print_exc(e)
        finally:
            db_util.close()


    def fail_action(self, values):
        '''
        1）更新hainiu_queue表中消费失败的错误次数和失败ip。
        2）如果当前的action对象的重试次数已经达到队列最大重试次数 ，
        将该记录设置is_work = 0, 待下次拿取重试（如果屏蔽ip的查询，下次就换其他机器爬取）。
        '''
        update_sql1 = """
        update hainiu_queue set fail_ip=%s, fail_times=fail_times+1 where id=%s;
        """

        update_sql2 = "update hainiu_queue set is_work=0 where id=%s;"

        db_util = DBUtil(_HAINIU_DB)
        try:
            fail_ip = Util().get_local_ip()
            sql_params = [fail_ip,values[0]]
            db_util.execute_no_commit(update_sql1, sql_params)

            # 当c_action 的当前重试次数达到最大重试，需要把is_work=0,再次重试
            if self.current_retry_num == _QUEUE_HAINIU['C_RETRY_TIMES'] - 1:
                sql_params = [values[0]]
                db_util.execute_no_commit(update_sql2, sql_params)

            db_util.commit()

        except Exception,e:
            db_util.rollback()
            traceback.print_exc(e)
        finally:
            db_util.close()

if __name__ == '__main__':
    # 防止出现 UnicodeDecodeError: 'ascii' codec can't decode byte 0xd6 in position 0: ordinal not in range(128)
    reload(sys)
    sys.setdefaultencoding('utf-8')

    p_action = HainiuProducerAction()
    queue = Queue.Queue()
    producer = Producer(queue,
                        _QUEUE_HAINIU['NAME'],
                        p_action,
                        _QUEUE_HAINIU['P_SLEEP_TIME'],
                        _QUEUE_HAINIU['C_MAX_NUM'],
                        _QUEUE_HAINIU['C_MAX_SLEEP_TIME'],
                        _QUEUE_HAINIU['C_RETRY_TIMES']
                        )
    producer.start_work()
    # -----单线程调试-----
    # p_action = HainiuProducerAction()
    # c_actions = p_action.queue_items()
    # for c_action in c_actions:
    #     c_action.action()