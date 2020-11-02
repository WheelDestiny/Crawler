#_*_coding:utf-8_*_
import sys,traceback,Queue,datetime,os,shutil,json
sys.path.append('/home/wheeldestiny26/hainiu_crawler')

from Util.db_util import DBUtil
from Util.html_util import HtmlUtil
from Util.request_util import RequestUtil
from Util.time_util import TimeUtil
from Util.util import Util
from commons.action.base_consumer_action import ConsumerAction
from commons.action.base_producer_action import ProducerAction
from Config.config import _HAINIU_DB,_QUEUE_PRODUCE_NEWS,_LOCAL_DATA_DIR
from commons.action.producer import Producer

reload(sys)
sys.setdefaultencoding("utf-8")
# 下载新闻，从queue向web_page传输，
# 从queue获取链接，通过工具下载数据，导入到web_page中
class NewsDownloadProducerAction(ProducerAction):

    def queue_items(self):
        '''
        查询 wheelDestiny_hainiu_queue 表，生成NewsDownloadConsumerAction对象列表，
        :return:
        '''
        # 查询queue，type=2，未被使用，屏蔽ip
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
            sql_params = [2,0,ip,_QUEUE_PRODUCE_NEWS['MAX_FAIL_TIMES'],_QUEUE_PRODUCE_NEWS['LIMIT_NUM']]
            # 获取查询结果
            result = db_util.read_dict(select_sql,sql_params)
            # 遍历结果集，生产NewsDownloadConsumerAction，并添加到c_actions中
            for row in result:
                id = row['id']
                ids.append(str(id))
                url = row['action']
                params = row['params']
                c_action = NewsDownloadConsumerAction(id,url,params)
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

class NewsDownloadConsumerAction(ConsumerAction):
    def __init__(self,id,url,params):
        super(self.__class__,self).__init__()
        self.id = id
        self.url = url
        self.params = params

    def action(self):
        '''
        把队列中的url的HTML经过处理后写入到文件中，每个消费线程每隔5分钟生成一个新的文件，防止某个文件过大

        如果过程顺利写入，把已经写入的文件的数据传入wheelDestiny_hainiu_web_page

        要传入web_page的字段，url，md5，param，domain,host,title,create_time,create_day,hour,update_time

        :return:
        '''

        # 准备要使用的工具类
        req_util = RequestUtil()
        html_util = HtmlUtil()
        util = Util()
        time_util = TimeUtil()
        db_util = DBUtil(_HAINIU_DB)

        create_time = time_util.get_timestamp()
        # 年月日
        create_day = int(time_util.now_day(format='%Y%m%d'))
        # 小时
        hour = int(time_util.now_hour())
        # 分钟
        minute= int(time_util.now_min())

        update_time = create_time

        #种子url的md5
        urlMD5 = util.get_md5(self.url)
        # 设置爬取结果标记
        is_success = True

        try:
            #爬取 hainiu_queue 中符合要求的url,将爬取的html处理后写入文件
            thread_name = self.current_thread_name

            #通过phandomjs请求种子url得到HTML页面
            html = req_util.http_get_phandomjs(self.url)

            if html == '':
                is_success = False

            # 种子url的domain
            domain = html_util.get_url_domain(self.url)
            # 种子url的host
            host = html_util.get_url_host(self.url)

            '''
            把html处理好，写入开始
            '''
            # 处理html,转化为一行数据
            html_as_a_line = str(html).replace('\r',' ').replace('\n','\002')

            # 把url和html拼在一起，并获取到md5，最终拼在一起得到最后要写入文件的数据
            url_html = self.url+'\001'+html_as_a_line

            url_html_md5 = util.get_md5(url_html)
            # 最终得到要写的一行数据 md5+\001+url+\001+html
            write_line = url_html_md5+'\001'+url_html

            # 准备要写入的文件名
            for i in range(60,-5,-5):
                if minute < i:
                    continue
                minute = i
                break

            # 格式化时间
            minute = '0%s' % minute if minute < 10 else minute
            hour = '0%s' % hour if hour < 10 else hour

            # print '%s%s%s' % (day,hour,minute)
            now_minute = '%s%s%s' % (create_day,hour,minute)
            # 当前要写入的文件名
            now_file_name = "%s_%s" % (thread_name, now_minute)
            # 写入文件的目录
            tmp_path = _LOCAL_DATA_DIR % 'tmp'
            # 已经写入完成的文件目录
            done_path = _LOCAL_DATA_DIR % 'done'
            # tmp目录下的文件列表
            # （程序正常运作的情况下，tmp目录一条线程只会有一个文件，追加就继续写入，如果是写入新文件，则把tmp中的文件转移到done目录下）
            file_names = os.listdir(tmp_path)

            last_file_name = ''
            for file_name in file_names:
                # 根据线程名称获取到上一个写入的文件
                if file_name.__contains__(thread_name):
                    last_file_name = file_name
            # 如果旧文件名不为空且和当前要写入的文件名不相同
            # 把旧文件移动到done目录
            if last_file_name != '' and last_file_name != now_file_name:
                src = os.path.join(tmp_path, last_file_name)
                dst = os.path.join(done_path, last_file_name)
                shutil.move(src, dst)
            # 写入文件
            with open(tmp_path+'/'+now_file_name, 'a') as f:
                f.writelines(write_line+'\n')

            '''
            写入文件结束
            '''
            params_dict = json.loads(str(self.params))
            a_title = params_dict['a_title']

            # 写入文件之后，把本条数据插入web_page
            # url，md5，param，domain,host,title,create_time,create_day,hour,update_time
            # 根据写入结果的标记，设定status的状态，1为成功，2为失败
            status = 1 if is_success else 2
            row_data = (self.url,
                        urlMD5,
                        self.params,
                        domain,
                        host,
                        a_title,
                        create_time,
                        create_day,
                        hour,
                        update_time,
                        status)
            IOU_sql = '''
            insert into wheelDestiny_hainiu_web_page 
            (url,md5,param,domain,host,title,create_time,create_day,create_hour,update_time,status) 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
            on DUPLICATE KEY UPDATE fail_times = fail_times;
            '''

            db_util.execute(IOU_sql,row_data)

        except Exception,e:
            traceback.print_exc(e)
            is_success = False
            db_util.rollback()
        finally:
            req_util.close_phandomjs()
            db_util.close()

        return self.result(is_success,self.id,urlMD5,create_time)

    def success_action(self,values):
        # 如果消费成功，
        # 删除queue表的数据
        # 更新内链表的最后爬取时间，

        db_util = DBUtil(_HAINIU_DB)

        delete_sql = '''
        delete from wheelDestiny_hainiu_queue where id = %s
        '''

        update_internally_sql = '''
        update wheelDestiny_hainiu_web_seed_internally
        set update_time = %s
        where a_md5 = %s;
        '''

        try:
            # 删除queue表的数据
            delete_params = [values[0]]
            db_util.execute_no_commit(delete_sql,delete_params)
            # 更新内链表的最后爬取时间
            update_params = [int(values[2]),values[1]]
            db_util.execute_no_commit(update_internally_sql,update_params)

            db_util.commit()

        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

    def fail_action(self, values):
        # 如果消费失败
        # 更新queue表，错误次数+1，记录当前ip
        # 当某个ip的错误次数到达设定的最大值，重新放入队列，让其他的ip尝试
        # 更新内链表的失败次数和失败ip

        db_util = DBUtil(_HAINIU_DB)
        update_queue_sql1 = '''
        update wheelDestiny_hainiu_queue set fail_ip = %s,fail_times = fail_times+1 where id = %s
        '''
        update_queue_sql2 = '''
        update wheelDestiny_hainiu_queue set is_work = 0 where id = %s
        '''
        update_internal_sql = '''
        update wheelDestiny_hainiu_web_seed_internally set fail_times = fail_times+1,fail_ip = %s where a_md5 = %s
        '''
        try:
            # 更新队列表的失败次数和失败IP
            fail_ip = Util().get_local_ip()
            sql_params = [fail_ip,values[0]]
            db_util.execute_no_commit(update_queue_sql1,sql_params)

            # 如果失败次数达到最大，把对象从列表中放出，把is_work=0
            if self.current_retry_num == _QUEUE_PRODUCE_NEWS['C_RETRY_TIMES'] -1:
                db_util.execute_no_commit(update_queue_sql2 % values[0])
            # 更新内链表的失败次数和失败ip
            sql_params = [fail_ip,values[1]]
            db_util.execute_no_commit(update_internal_sql,sql_params)

            db_util.commit()
        except Exception,e:
            traceback.print_exc(e)
            db_util.rollback()
        finally:
            db_util.close()

if __name__ =='__main__' :

    queue = Queue.Queue()
    p_action = NewsDownloadProducerAction()

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


