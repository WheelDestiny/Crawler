#-*- encoding: utf-8 -*-
'''
add_seed_to_queue.py
Created on 20-10-17 下午4:46
Copyright (c) 20-10-17, 海牛学院版权所有.
@author: 潘牛
'''
import sys,time,json
from Config.config import _QUEUE_HAINIU,_HAINIU_DB
from Util.db_util import DBUtil
from Util.log_util import LogUtil


def put_queue(page_show_num):

    # 查询队列表符合条件的总记录数
    select_queue_count_sql = """
    select count(*) from wheelDestiny_hainiu_queue where type=%s and is_work=0 and fail_times=0;
    """
    # 查询种子表符合条件的总记录数
    select_seed_count_sql = """
    select count(*) from wheelDestiny_hainiu_web_seed where status=0;
    """

    # 查询分页SQL
    select_seed_limit_sql = """
    select url, md5, domain, host, category from wheelDestiny_hainiu_web_seed where status=0 limit %s, %s;
    """
    # 插入队列表的SQL
    insert_queue_sql = """
    insert into wheelDestiny_hainiu_queue (type,action,params) values (%s, %s, %s);
    """

    db_util = DBUtil(_HAINIU_DB)
    logger = LogUtil().get_logger("put_hainiu_queue", "put_hainiu_queue")

    try:

        start_time = time.time()

        sql_params = [1]
        res1 = db_util.read_one(select_queue_count_sql, sql_params)
        queue_count = res1[0]
        # 如果队列表有10条没处理，那就不导入数据
        if queue_count >= 10:
            logger.info("当前队列表有 %d 条数据，不需要导入数据" % queue_count)
            return None

        res2 = db_util.read_one(select_seed_count_sql)
        seed_count = res2[0]

        # 计算分页数
        page_num = seed_count / page_show_num if seed_count % page_show_num == 0 \
            else seed_count / page_show_num + 1

        # 分页查询
        for i in range(page_num):
            sql_params = [i * page_show_num, page_show_num]
            res3 = db_util.read_dict(select_seed_limit_sql, sql_params)
            #({},{},{},{},{})
            insert_values = []

            for row in res3:
                #url, md5, domain, host, category
                url = row['url']
                md5 = row['md5']
                domain = row['domain']
                host = row['host']
                category = row['category']
                params_dict = {}
                params_dict['main_md5'] = md5
                params_dict['domain'] = domain
                params_dict['host'] = host
                params_dict['category'] = category
                # 把字典的数据序列化成json字符串
                param_json = json.dumps(params_dict,ensure_ascii=False, encoding='utf-8')
                row_data = (1, url, param_json)
                insert_values.append(row_data)
            # 把查询的5条数据一次性插入到队列表里
            db_util.executemany(insert_queue_sql, insert_values)

        end_time = time.time()
        run_time = end_time - start_time
        logger.info("本地导入 %d 条数据， 运行时长：%.2f 秒" % (seed_count, run_time))
    except Exception,e:
        logger.exception(e)
    finally:
        db_util.close()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    put_queue(_QUEUE_HAINIU['PAGE_SHOW_NUM'])
