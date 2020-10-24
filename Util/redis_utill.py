# -*- encoding: utf-8 -*-
'''
redis_utill.py
Created on 2017/6/30 16:06
Copyright (c) 2017/6/30, 海牛学院版权所有.
@author: 青牛
'''

import threading, time

from rediscluster import RedisCluster
# from redis.connection import (ConnectionPool, UnixDomainSocketConnection,SSLConnection, Token)
import sys, json,redis


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

        return RedisUtill._instance

    def creat_conn(self):
        redis_nodes = [{'host': 'nn1.hadoop', 'port': 6379},
                       {'host': 'nn2.hadoop', 'port': 6379},
                       {'host': 's1.hadoop', 'port': 6379},
                       {'host': 's2.hadoop', 'port': 6379},
                       {'host': 's3.hadoop', 'port': 6379},
                       {'host': 's4.hadoop', 'port': 6379}]
        try:
            redisconn = RedisCluster(startup_nodes=redis_nodes)

        except Exception:
            print "Connect Error!"
            sys.exit(1)

        self.redisconn = redisconn
        return redisconn

    def get_conn(self):
        '''
        获取 redis 集群链接

        如果之前链接无效重新链接  --- 防止频繁 重新链接集群
        :return:    创建的链接
        '''
        if not hasattr(self, "redisconn"):
            RedisUtill.redisconn = self.creat_conn()
            self.redisconn = RedisUtill.redisconn
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

    def get_lock(self,lock_key, timeout=10, max_timeout=100):
        """
        获取锁
        lock_key ： 锁的名称
        timeout : 失效时间  ---- 自动解锁的时间   单位 秒
        max_timeout：最大超时时间， 当超过最大超时时间后，退出获取锁的逻辑，并返回False
        """

        start_time = time.time()
        is_get_lock = False
        while True:
            # SETNX 是SET if Not eXists的简写，当key不存在时设置值并返回True；key存在时不设置并返回False
            is_lock = self.get_conn().setnx(lock_key, time.time() + timeout)
            if not is_lock:
                time.sleep(0.5)
                # 判断最大超时时间
                # 返回秒
                end_time = time.time()
                if end_time - start_time > max_timeout:
                    print '尝试时间超过最大尝试时间，未获取到锁，退出'
                    is_get_lock = False
                    break

                continue
            else:
                is_get_lock = True
                # 设置超时timeout时间后，key自动删除
                self.get_conn().expire(lock_key, timeout)
                break

        return is_get_lock



    def release(self, lock_key):
        return self.get_conn().delete(lock_key)


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # g = RedisUtill()
    # dicts = {'key1': 11, 'key2': 22, 'key3': 'aaa'}
    # s = g.set_batch_datas(dicts)
    # print s
    # list = ['key1', 'key2', 'key3']
    # # 根据keys(列表) 返回values（列表）
    # dd = g.get_values_batch_keys(list)
    # print dd
    #
    # print '-----------------\n解决中文乱码问题'
    # # 解决中文乱码问题
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
    #
    # # ['wo', '修改成中文', '大家', '中文']
    # g.set_data('testxiugai', result)
    # std2 = g.get_value_for_key('testxiugai')
    # ddddd2 = json.loads(std2)
    # for i in ddddd2:
    #     print i

    # print '---------------------'
    # g = RedisUtill()
    # g.set_data('hainiu:key1', 123)
    # g.set_data('hainiu:key2', 33)
    # dlist = ['hainiu:key1', 'hainiu:key2']
    # print g.get_values_batch_keys(dlist)
    # list = ('key1', 'key2', 'key3')
    # # 批量获取keys 的values
    # dd = g.get_values_batch_keys(list)
    # # 删除keys 的数据
    # g.delete_batch(list)
    # # 再获取keys 的values 时，得到的是None
    # dd2 = g.get_values_batch_keys(list)
    # print dd
    # print dd2

    #
    # print '-----清理数据----------------'
    # # 清理数据
    # g = RedisUtill()
    # dd = g.get_conn().keys('*')
    # s = g.delete_batch(dd)
    #
    # print '-----造数据----------------'
    # ru = RedisUtill()
    # for i in range(1, 10):
    #     ru.set_data('down:md5%d' % i, "url%s" % i)


    # print '-----scan limit----------------'
    # limit_keys_obj = {}
    # ru = RedisUtill()
    # limit_keys_obj = ru.get_conn().scan(cursor=0, match='inner:*', count=4)
    # for k, v in limit_keys_obj.items():
    #     print k,v

    # host = '192.168.142.162'
    # port = '6379'
    # r = redis.Redis(host, port)
    # rs = r.scan(2, 'inner:*', 2)
    # print rs


    # 读取多台机器的down开头的key
    ips = ['192.168.142.160', '192.168.142.161', '192.168.142.162']
    port = '6379'
    def scan_limit_to_queue_table(host, port, cursor, match, count):
        r = redis.Redis(host, port)
        rs = r.scan(cursor, match, count)
        next_num = rs[0]
        print rs
        # ru.delete_batch(rs[1])
        if next_num == 0:
            return None
        scan_limit_to_queue_table(host, port, next_num, match, count)

    # scan_limit_to_queue_table(ips[0], port, 0,'down:*', 2)
    for ip in ips:

        scan_limit_to_queue_table(ip, port, 0,'down:*', 2)
        print '======'




    #

    # print '------测试是否为单例---------------'
    ## 单线程测试单例
    # obj1 = RedisUtill()
    #
    # obj1.get_conn()
    # obj2 = RedisUtill()
    # obj2.get_conn()
    # print(obj1,obj2)
    # 多线程方式测试是否为单例
    # def task(arg):
    #     obj = RedisUtill()
    #     print(obj)
    #
    # for i in range(10):
    #     t = threading.Thread(target=task,args=[i,])
    #     t.start()
