#-*- encoding: utf-8 -*-
'''
发现新闻
'''
import json
import sys,traceback,Queue,datetime

from Util.redis_utill_alone import RedisUtill

sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from bs4 import BeautifulSoup
from Util.html_util import HtmlUtil
from Util.request_util import RequestUtil
from Util.time_util import TimeUtil
from commons.action.base_producer_action import ProducerAction
from commons.action.base_consumer_action import ConsumerAction
from Util.db_util import DBUtil
from Config.config import _HAINIU_DB,_QUEUE_PRODUCE_NEWS
from Util.util import Util
from commons.action.producer import Producer


class NewsFindProducerAction(ProducerAction):
    def queue_items(self):
        '''
        查询 wheelDestiny_hainiu_queue 表，生成NewsFindConsumerAction对象列表
        :return:
        '''

        #查询sql，悲观锁，因为集群运行单线程，就不屏蔽ip了，没有意义
        # ,算了为了程序完整性，写上吧
        select_sql = '''
        select id,action,params 
        from wheelDestiny_hainiu_queue 
        where type = %s and is_work = %s and fail_ip!=%s and fail_times<%s limit %s 
        for update;
        '''

        # 更新 wheelDestiny_hainiu_queue 表，标记该条数据已经被使用过
        update_sql = '''
        update wheelDestiny_hainiu_queue set is_work=1 where id in (%s);
        '''
        # 用于装生产出的对象的容器
        c_actions = []
        # 用于记录所有被使用过的数据的id
        ids = []
        # 创建数据库连接
        db_util =DBUtil(_HAINIU_DB)
        try:
            # 获取当前ip
            ip = Util().get_local_ip()
            sql_params = [1,0,ip,_QUEUE_PRODUCE_NEWS['MAX_FAIL_TIMES'],_QUEUE_PRODUCE_NEWS['LIMIT_NUM']]
            # 获取查询结果
            result = db_util.read_dict(select_sql,sql_params)
            # 遍历结果集，生产NewsFindConsumerAction，并添加到c_actions中
            for row in result:
                id = row['id']
                ids.append(str(id))
                url = row['action']
                params = row['params']
                c_action = NewsFindConsumerAction(id,url,params)
                c_actions.append(c_action)
            # 如果生成的数据量大于1，修改被使用过的数据的状态，避免重复使用
            if len(ids)>0:
                db_util.execute_no_commit(update_sql % ','.join(ids))

            db_util.commit()

        except Exception,e:
            db_util.rollback()
            traceback.print_exc(e)
        finally:
            db_util.close()
        return c_actions


class NewsFindConsumerAction(ConsumerAction):

    def __init__(self,id,url,params):
        super(self.__class__,self).__init__()
        self.id = id
        self.url = url
        self.params = params

    def action(self):
        # 准备要使用的工具类
        req_util = RequestUtil()
        html_util = HtmlUtil()
        util = Util()
        time_util = TimeUtil()
        redis_util = RedisUtill()
        db_util = DBUtil(_HAINIU_DB)

        create_time = time_util.get_timestamp()
        # 年月日
        create_day = int(time_util.now_day(format='%Y%m%d'))
        # 小时
        hour = int(time_util.now_hour())
        update_time = create_time
        #种子url最后成功爬取的时间
        last_crawl_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        last_crawl_inner_num = 0
        last_crawl_external_num = 0

        #种子url的md5
        urlMD5 = util.get_md5(self.url)
        # 设置爬取结果标记
        is_success = True
        try:
            #爬取 hainiu_queue 中符合要求的url 请求页面的所有 a标签url ，

            #通过phandomjs请求种子url得到HTML页面
            html = req_util.http_get_phandomjs(self.url)
            #通过BeautifulSoup把HTML页面转DOM对象
            soup = BeautifulSoup(html,'lxml')
            # a链接dom对象列表
            a_docs = soup.find_all('a')
            # 如果种子url对应的HTML页面没有a链接，说明爬取失败,或者种子url页面有问题
            if len(a_docs) == 0 or html == '':
                is_success = False


            # 种子url的domain
            domain = html_util.get_url_domain(self.url)
            # 种子url的host
            host = html_util.get_url_host(self.url)
            # 准备一个set用来存放所有的a标签中的链接，用set的原因是为了去重
            a_set = set([])
            # 内链表列表
            inner_list = []
            # 外链表列表
            external_list = []
            # redis表exists_list字典
            redis_exist_list = {}
            # redis表down_list字典
            redis_down_list = {}

            already_exists_list = [];
            redis_util.scan_limit_to_queue_table(0,'exist*', 20, already_exists_list)

            # 循环所有的a标签，区分出内外链，放入列表
            for a_doc in a_docs:
                # 获取到a标签链接的具体href内容
                a_href = html_util.get_format_url(self.url,a_doc,host)
                # 获取a标签的title
                a_title = a_doc.get_text().strip()
                # 如果这个a标签的href或者title为空，
                # 或者如果这个a标签已经被使用过了，已经在链表中了，
                # 即跳过这个a标签，继续操作下一个a标签
                if a_href == '' or a_title == '' or a_set.__contains__(a_href):
                    continue

                a_set.add(a_href)
                a_md5 = util.get_md5(a_href)
                a_host = html_util.get_url_host(a_href)
                a_xpath = html_util.get_dom_parent_xpath_js_new(a_doc)

                # 设计redis的key，两个key，exist，down
                redisMD5 = util.get_md5(urlMD5 + a_md5)
                existskey = 'exists:'+redisMD5
                downkey = 'down:'+redisMD5

                # 封装一个字典，转成json用来保存到redis
                redis_data = {
                    'url':self.url,
                    'md5':urlMD5,
                    'param':self.params,
                    'domain':domain,
                    'host':host,
                    'a_url':a_href,
                    'a_md5':a_md5,
                    'a_host':a_host,
                    'a_xpath':a_xpath,
                    'a_title':a_title,
                    'create_time':create_time,
                    'create_day':create_day,
                    'create_hour':hour,
                    'update_time':update_time
                }
                redis_data_json = json.dumps(redis_data,ensure_ascii=False, encoding='utf-8')

                # 封装一行数据到一个对象中
                row_data = (self.url,
                            urlMD5,
                            self.params,
                            domain,
                            host,
                            a_href,
                            a_md5,
                            a_host,
                            a_xpath,
                            a_title,
                            create_time,
                            create_day,
                            hour,
                            update_time)
                # 如果href中包含主站的domain，则为内链接，加入内链表，反之，加入外链表
                if a_href.__contains__(domain):
                    inner_list.append(row_data)
                    if (existskey not in already_exists_list):
                        redis_exist_list[existskey] = redis_data_json
                        redis_down_list[downkey] = redis_data_json
                else:
                    external_list.append(row_data)



            # 并解析存入内链表或外链表，在存入时，如果url已存在，只做update 操作。（保证链接页面不会重复爬取）
            # 根据内外链情况，批量插入插入/更新到内链表，如果当前链接没有被爬过，执行插入，如果当前链接已经被爬过，在表中有记录，则执行更新
            IOU_sql = '''
            insert into <table_name> (url,md5,param,domain,host,a_url,a_md5,a_host,a_xpath,a_title,create_time,create_day,create_hour,update_time) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on DUPLICATE KEY UPDATE fail_times = fail_times;
            '''
            if len(inner_list) > 0:
                inner_sql = IOU_sql.replace('<table_name>','wheelDestiny_hainiu_web_seed_internally')
                db_util.executemany_no_commit(inner_sql,inner_list)
                '''
                开始导入redis
                每条数据保存两份，
                    key开头的标记分别为exists(永久保存)，down(去重使用，在内链表导入队列表后将down标记的key删除)
                    举例exists:md5(md5,a_md5),down:md5(md5,a_md5)
                    value的数据格式设置为json格式化后的字典
                '''
                if len(redis_exist_list)>0:
                    # print 'len(redis_exist_list)>0'
                    redis_util.set_batch_datas(redis_exist_list)
                    redis_util.set_batch_datas(redis_down_list)
                '''
                导入redis结束
                '''
            if len(external_list) > 0:
                external_sql = IOU_sql.replace('<table_name>','wheelDestiny_hainiu_web_seed_externally')
                db_util.executemany_no_commit(external_sql,external_list)

            # 记录内链外链各记录了多少数据
            last_crawl_inner_num = len(inner_list)
            last_crawl_external_num = len(external_list)

            db_util.commit()
        except Exception,e:
            traceback.print_exc(e)
            is_success = False
            db_util.rollback()
        finally:
            req_util.close_phandomjs()
            db_util.close()

        return self.result(is_success,self.id,urlMD5,last_crawl_time,last_crawl_inner_num,last_crawl_external_num)

    def success_action(self,values):
        db_util = DBUtil(_HAINIU_DB)

        delete_sql = '''
        delete from wheelDestiny_hainiu_queue where id = %s
        '''
        update_sql = '''
        update wheelDestiny_hainiu_web_seed
        set last_crawl_time = %s,last_crawl_internally = %s,last_crawl_externally = %s 
        where md5 = %s;
        '''

        try:
            # 删除已经被成功爬取的数据
            delete_params = [values[0]]
            db_util.execute_no_commit(delete_sql,delete_params)
            # 更新对应seed表中url的最后爬取时间，爬取的内链数量，外链数量
            update_params = [values[2],values[3],values[4],values[1]]
            db_util.execute_no_commit(update_sql,update_params)

            db_util.commit()

        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

    def fail_action(self, values):
        # 1）记录hainiu_queue表错误次数和ip；
        # 2）当某个机器的错误次数达到了当前机器设定的最大重试次数，把hainiu_queue 表对应的记录的 is_work = 0，让其他机器重试；
        # 3）更新种子表的失败次数、失败ip；队列表的数据不删除，有可能是因为目标网站把ip给封了， 在某个时间，写个脚本，把失败的队列数据改状态和失败次数和失败ip，重新爬取试试。

        # 消费失败首先要更新失败ip和失败次数
        update_sql1 = '''
        update wheelDestiny_hainiu_queue set fail_ip = %s,fail_times = fail_times+1 where id = %s
        '''
        # 如果达到失败次数上限，把它从列表中放出，交给其他ip尝试
        update_sql2 = '''
        update wheelDestiny_hainiu_queue set is_work = 0 where id = %s
        '''
        # 更新种子表，更新失败次数，失败ip
        update_sql3 = '''
        update wheelDestiny_hainiu_web_seed set fail_times = fail_times+1,fail_ip = %s where md5 = %s
        '''
        db_util = DBUtil(_HAINIU_DB)

        try:
            # 更新队列表的失败次数和失败IP
            fail_ip = Util().get_local_ip()
            sql_params = [fail_ip,values[0]]
            db_util.execute_no_commit(update_sql1,sql_params)

            # 如果失败次数达到最大，把对象从列表中放出，把is_work=0
            if self.current_retry_num == _QUEUE_PRODUCE_NEWS['C_RETRY_TIMES'] -1:
                db_util.execute_no_commit(update_sql2 % values[0])

            # 更新种子表，记录失败次数和失败ip
            sql_params = [fail_ip,values[1]]
            db_util.execute_no_commit(update_sql3,sql_params)

            db_util.commit()
        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")

    queue = Queue.Queue()
    p_action = NewsFindProducerAction()

    produce = Producer(
        queue,
        _QUEUE_PRODUCE_NEWS['NAME'],
        p_action,
        _QUEUE_PRODUCE_NEWS['P_SLEEP_TIME'],
        _QUEUE_PRODUCE_NEWS['C_MAX_NUM'],
        _QUEUE_PRODUCE_NEWS['C_MAX_SLEEP_TIME'],
        _QUEUE_PRODUCE_NEWS['C_RETRY_TIMES']
    )

    produce.start_work()



