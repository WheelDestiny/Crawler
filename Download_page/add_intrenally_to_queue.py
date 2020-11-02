#-*- encoding: utf-8 -*-
'''
add_seed_to_queue.py
Created on 20-10-17 下午4:46
Copyright (c) 20-10-17, 海牛学院版权所有.
@author: 潘牛
'''
import sys,time,json

from Util.redis_utill_alone import RedisUtill

sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from Config.config import _QUEUE_HAINIU,_HAINIU_DB
from Util.db_util import DBUtil
from Util.log_util import LogUtil


def put_queue(page_show_num):
    # wheelDestiny_hainiu_web_seed_internally to wheelDestiny_hainiu_queue
    # 查询queue表符合条件的总记录数
    select_queue_count_sql = """
    select count(*) from wheelDestiny_hainiu_queue where type=%s and is_work=0 and fail_times=0;
    """
    # 查询internally表符合条件的总记录数
    select_internally_count_sql = """
    select count(*) from wheelDestiny_hainiu_web_seed_internally where status=0;
    """

    # 分页查询internally SQL
    select_internally_limit_sql = """
    select id,url,a_url, md5, domain, host, a_title from wheelDestiny_hainiu_web_seed_internally where status=0 limit %s, %s;
    """
    # 修改已经被插入queue表的数据的状态
    update_internally_status = '''
    update wheelDestiny_hainiu_web_seed_internally set status =1 where concat(md5,a_md5) in (%s);
    '''

    # 插入queue表的SQL
    insert_queue_sql = """
    insert into wheelDestiny_hainiu_queue (type,action,params) values (%s, %s, %s);
    """
    ru = RedisUtill()
    db_util = DBUtil(_HAINIU_DB)
    logger = LogUtil().get_logger("put_internally_to_queue", "put_internally_to_queue")

    try:

        start_time = time.time()

        sql_params = [2]
        res1 = db_util.read_one(select_queue_count_sql, sql_params)
        queue_count = res1[0]
        # 如果queue表有10条以上没处理，那就不导入数据
        if queue_count >= 10:
            logger.info("当前queue表有 %d 条internally表数据，不需要导入数据" % queue_count)
            return None

        # res2 = db_util.read_one(select_internally_count_sql)
        # internally_count = res2[0]

        # ids = []
        # # 计算分页数
        # page_num = internally_count / page_show_num if internally_count % page_show_num == 0 \
        #     else internally_count / page_show_num + 1

        # 分页查询
        # for i in range(page_num):
        #     sql_params = [i * page_show_num, page_show_num]
        #     res3 = db_util.read_dict(select_internally_limit_sql, sql_params)
        #     insert_values = []
        #
        #     # 记录每次被查出的数据，在成功插入queue后将这些数据的状态status改为1，表示已经下载过，不再爬取了
        #
        #     for row in res3:
        #         #url, md5, domain, host, category
        #         id = row['id']
        #         ids.append(str(id))
        #         a_url = row['a_url']
        #         md5 = row['md5']
        #         domain = row['domain']
        #         host = row['host']
        #         a_title = row['a_title']
        #         params_dict = {}
        #         params_dict['main_md5'] = md5
        #         params_dict['domain'] = domain
        #         params_dict['host'] = host
        #         params_dict['a_title'] = a_title
        #         # 把字典的数据序列化成json字符串
        #         param_json = json.dumps(params_dict,ensure_ascii=False, encoding='utf-8')
        #         row_data = (2, a_url, param_json)
        #         insert_values.append(row_data)
        #         # if len(insert_values)>0:
        #         # 把查询的5条数据一次性插入到队列表里
        #     db_util.executemany(insert_queue_sql, insert_values)

        # 记录已经导入queue表的redis表数据，用来更新internally表的status
        md5_amd5 = []

        down_list = []
        insert_values = []
        ru.scan_limit_to_queue_table(0,'down*', 20, down_list)
        for down_data in down_list:

            value = ru.get_value_for_key(down_data)
            params_dict = json.loads(value)
            a_url = params_dict['a_url']
            a_title = params_dict['a_title']
            md5 = params_dict['md5']
            a_md5 = params_dict['a_md5']
            domain = params_dict['domain']
            host = params_dict['host']
            params_dict = {}
            params_dict['main_md5'] = md5
            params_dict['domain'] = domain
            params_dict['host'] = host
            params_dict['a_title'] = a_title

            md5_amd5.append(str("'"+md5+a_md5+"'"))

            param_json = json.dumps(params_dict,ensure_ascii=False, encoding='utf-8')

            row_data = (2, a_url, param_json)
            insert_values.append(row_data)
        if len(down_list)>0:

            db_util.executemany_no_commit(insert_queue_sql, insert_values)

            # 插入成功后把internally表的status改为1，让已经进入queue的数据不再进入queue
            db_util.execute_no_commit(update_internally_status % ','.join(md5_amd5))
            # 并且删除redis中down开头的key
            ru.delete_batch(down_list)

            db_util.commit()
        end_time = time.time()
        run_time = end_time - start_time
        logger.info("本地导入 %d 条数据， 运行时长：%.2f 秒" % (len(down_list), run_time))
    except Exception,e:
        logger.exception(e)
    finally:
        db_util.close()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    put_queue(_QUEUE_HAINIU['PAGE_SHOW_NUM'])
