# -*- encoding: utf-8 -*-
'''
redis_utill.py
Created on 2017/6/30 16:06
Copyright (c) 2017/6/30, 海牛学院版权所有.
@author: 潘牛
'''

import threading, time,sys, json,redis

# from redis.connection import (ConnectionPool, UnixDomainSocketConnection,SSLConnection,Token)


class RedisUtill(object):

    _instance_lock = threading.Lock()



    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        '''
        单例模式
        '''
        if not hasattr(RedisUtill, "_instance"):
            with RedisUtill._instance_lock:
                if not hasattr(RedisUtill, "_instance"):
                    RedisUtill._instance = object.__new__(cls)
                    # 创建redis连接池
                    RedisUtill._pool = redis.ConnectionPool(host='nn1.hadoop', port=6379, db=13, max_connections=10)

        return RedisUtill._instance


    def get_conn(self):
        '''
        获取 redis 集群链接

        如果之前链接无效重新链接  --- 防止频繁 重新链接集群
        :return:    创建的链接
        '''
        self.redisconn = redis.Redis(connection_pool=RedisUtill._pool)
        return self.redisconn

    def keys_limit_scan(self, pattern='*', limit=1, cursor=0):
        '''
        批量获取 keys
        '''
        limit_keys_obj = self.get_conn().scan(cursor, pattern, limit)
        limit_keys_list = []
        for key, value in limit_keys_obj.items():
            for i in value[1]:
                limit_keys_list.append(i)

        return limit_keys_list

    def get_values_batch_keys(self, keys):
        '''
        通过 keys 批量获取值values  --列表 []
        '''
        return self.get_conn().mget(keys)

    def get_value_for_key(self, key):
        '''
        通过 key  获取值   单个
        '''
        return self.get_conn().get(key)

    def set_data(self, key, value):
        '''
        保存单个值
        '''
        return self.get_conn().set(key, value)

    def set_batch_datas(self, keydicts):
        '''
        批量保存  c传入字典 {key:value,key2:value2}
        '''
        return self.get_conn().mset(keydicts)


    def delete_data(self, key):
        '''
        删除
        '''
        return self.get_conn().delete(key)

    def delete_batch(self, keys):
        '''
        批量删除   --- redis 的郁闷到奔溃  传，key 的列表  []
        '''
        for i in keys:
            self.get_conn().delete(i)

    def rename_key(self, src, dst_new):
        '''
        重命名 key
        '''
        return self.get_conn().rename(src, dst_new)

    def get_all_key_value(self):
        '''
        获取所用的数据  打印信息
        '''
        keys = self.get_conn().keys()
        for i in keys:
            print   i, ':', self.get_conn().get(i)


    def get_lock(self,lock_key, timeout=10 ,max_try_time=100):
        '''
        lock_key ： 锁的名称
        timeout : 失效时间  ---- 自动解锁的时间   单位 秒
        max_try_time ： 尝试获取锁的最大等待时间  单位 秒
        '''

        is_lock = self.get_conn().setnx(lock_key, time.time() + timeout)
        if is_lock:
            return True
        else:
            lock_value = self.get_conn().get(lock_key)
            # 锁超时
            if lock_value != None and time.time() > float(lock_value):
                self.get_conn().delete(lock_key)

            else:
                return self.get_conn().setnx(lock_key, time.time() + timeout)


    def release(self, lock_key):
        return self.get_conn().delete(lock_key)


    def scan_limit_to_queue_table(self,cursor, match, count,list):
        conn = self.get_conn()
        rs = conn.scan(cursor, match, count)
        next_num = rs[0]
        # print rs
        for key in rs[1]:
            list.append(key)
            self.get_values_batch_keys

        if next_num == 0:
            return None
        self.scan_limit_to_queue_table(next_num, match, count, list)


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    g = RedisUtill()
    value = g.get_value_for_key('down:000643804cbf0221eba36f7ed62c205b')
    params_dict = json.loads(value)
    a_title = params_dict['a_title']

    print a_title
    print value

    # g = RedisUtill()
    # dicts = {'key1': 11, 'key2': 22, 'key3': 'aaa'}
    # s = g.set_batch_datas(dicts)
    # print s
    # list = ['inner:md51', 'key2', 'key3']
    # # 根据keys(列表) 返回values（列表）
    # dd = g.get_values_batch_keys(list)
    # print dd
    #
    # print '-----------------\n解决中文乱码问题'
    # 解决中文乱码问题
    # g = RedisUtill()
    # # 重命名
    # sss = g.rename_key('key3', 'key4')
    # print g.get_value_for_key('key4'), sss
    #
    # list = ['wo', 'shi', '大家', '中文']
    # result = json.dumps(list, encoding='UTF-8', ensure_ascii=False)
    # g.set_data('testxiugai',result)
    #
    # std = g.get_value_for_key('testxiugai')
    # ddddd = json.loads(std)
    # ddddd[1] = '修改成中文'
    # result = json.dumps(ddddd, encoding='UTF-8', ensure_ascii=False)
    # g.set_data('testxiugai', result)
    # std2 = g.get_value_for_key('testxiugai')
    # ddddd2 = json.loads(std2)
    # for i in ddddd2:
    #     print i
    #
    # print '---------------------'
    # g.set_data('hainiu:key1', 123)
    # g.set_data('hainiu:key2', 33)
    # dlist = ['hainiu:key1', 'hainiu:key2']
    # print g.get_values_batch_keys(dlist)
    # list = ('key1', 'key2', 'key3')
    # 批量获取keys 的values
    # dd = g.get_values_batch_keys(list)
    # 删除keys 的数据
    # g.delete_batch(list)
    # 再获取keys 的values 时，得到的是None
    # dd2 = g.get_values_batch_keys(list)
    # print dd
    # print dd2


    # print '-----清理数据----------------'
    # 清理数据
    # g = RedisUtill()
    # dd = g.get_conn().keys('*')
    # s = g.delete_batch(dd)

    # print '-----造数据----------------'
    # ru = RedisUtill()
    # for i in range(1, 10):
    #     ru.set_data('inner:md5%d' % i, "url%s" % i)


    # print '-----scan limit----------------'
    # limit_keys_obj = {}
    # ru = RedisUtill()
    # limit_keys_obj = ru.get_conn().scan(cursor=0, match='down:*', count=4)
    # for k, v in limit_keys_obj.items():
    #     print k,v

    # host = 'nn1.hadoop'
    # port = '6379'
    # r = redis.Redis(host, port)
    # rs = r.scan(0, 'down:*', 4)
    # print rs




    # ru = RedisUtill()
    # def scan_limit_to_queue_table(cursor, match, count):
    #     r = ru.get_conn()
    #     rs = r.scan(cursor, match, count)
    #     next_num = rs[0]
    #     print rs
    #     # ru.delete_batch(rs[1])
    #     if next_num == 0:
    #         return None
    #     scan_limit_to_queue_table(next_num, match, count)
    #
    # scan_limit_to_queue_table(0,'*', 2)



    # ru = RedisUtill()
    # def scan_limit_to_queue_table(cursor, match, count,list):
    #     r = ru.get_conn()
    #     rs = r.scan(cursor, match, count)
    #     next_num = rs[0]
    #     print rs
    #     for key in rs[1]:
    #         list.append(key)
    #         ru.get_values_batch_keys
    #
    #     if next_num == 0:
    #         return None
    #     scan_limit_to_queue_table(next_num, match, count, list)

    # list = []
    # ru = RedisUtill()
    # ru.scan_limit_to_queue_table(0,'exist*', 20, list)
    # # 'exists:9ba03aa2b08d5627dcf01e0f697f2c04'
    #
    # print list
    # if ('exists:cf33a0de9c53542d8c6d232604bb66be' in list):
    #     print 'aa'


    # print len(list)


    # print '------测试是否为单例---------------'
    # # 多线程方式测试是否为单例
    # def task(arg):
    #     obj = RedisUtill()
    #     print(id(obj._pool))
    #
    # for i in range(10):
    #     t = threading.Thread(target=task,args=[i,])
    #     t.start()


    # import time
    # ru = RedisUtill()
    # timeout = 5

    # rs1 = ru.get_conn().setnx("key1", time.time() + timeout)
    # print rs1

    # rs2 = ru.get_conn().get("key1")
    #
    # if rs2 != None and time.time() - float(rs2) > 0:
    #     rs3 = ru.get_conn().delete("key1")
    #     print rs3

