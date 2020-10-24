#-*- encoding: utf-8 -*-
'''
log_util.py
Created on 2018/12/27 20:35
Copyright (c) 2018/12/27, 海牛学院版权所有.
@author: 潘牛
'''


import sys,logging,content
sys.path.append('/home/wheeldestiny26/hainiu_crawler')
from logging.handlers import TimedRotatingFileHandler
from Config import config


class LogUtil:

    base_logger = content._NULL_STR

    log_dict = {}

    def get_base_logger(self):
        if LogUtil.base_logger == content._NULL_STR:
            LogUtil.base_logger = self.__get_logger('info','info')
        return LogUtil.base_logger

    def get_logger(self,log_name,file_name):
        key = log_name + file_name
        if not LogUtil.log_dict.has_key(key):
            LogUtil.log_dict[key] = self.__get_logger(log_name,file_name)

        return  LogUtil.log_dict[key]

    def __get_new_logger(self,log_name,file_name):
        l = LogUtil()
        l.__get_logger(log_name,file_name)
        return l

    def __get_logger(self,log_name,file_name):
        self.logger = logging.getLogger(log_name)
        self.logger.setLevel(logging.INFO)

        # 按照时间滚动生成日志文件Handler
        fh = TimedRotatingFileHandler(config._LOG_DIR % (file_name),'D')
        # fh.suffix = "%Y%m%d.log"
        fh.setLevel(logging.INFO)

        # 控制台 Handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        return self

    def info(self,msg):
        self.logger.info(msg)
        self.logger.handlers[0].flush()

    def error(self,msg):
        self.logger.error(msg)
        self.logger.handlers[0].flush()

    def exception(self,msg='Exception Logged'):
        self.logger.exception(msg)
        self.logger.handlers[0].flush()


if __name__ == '__main__':
    import time
    while True:
        logger = LogUtil().get_base_logger()
        logger.info("111")
        logger.error("222")
        try:
            1/0
        except:
            logger.exception()
        time.sleep(1)



