#-*- encoding: utf-8 -*-
'''
new_seed.py.py
Created on 2017/10/19 11:21
Copyright (c) 2017/10/19, 海牛学院版权所有.
@author: 青牛
'''
import sys
sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from Util.log_util import LogUtil
from Util.db_util import DBUtil
from Util.html_util import HtmlUtil
from Config import config
from tld import get_fld
from Util.util import Util


def create_seed():
    url = "https://www.autohome.com.cn/all"
    catetory = "汽车"
    sql = """
    insert into hainiu_web_seed (url,md5,domain,host,category,status) values
    (%s,%s,%s,%s,%s,%s);
    """
    hu = HtmlUtil()
    domain = get_fld(url)
    host = hu.get_url_host(url)
    u = Util()
    md5 = u.get_md5(url)

    rl = LogUtil().get_base_logger()
    try:
        d = DBUtil(config._HAINIU_DB)
        sql_param = [url,md5,domain,host,catetory,0]
        d.execute(sql,sql_param)
    except:
        rl.exception()
        d.rollback()
    finally:
        d.close()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    create_seed()