#!python
# coding=utf8
from logging.handlers import RotatingFileHandler
import logging
import os
from conf import config

#启动脚本初始化一个日志名称，程序其它地方调用 main这个logger打印日志
def initLogger(fileName):
    log_file = os.path.join(config.logpath, fileName)
    fmt = '%(asctime)s - %(levelname)s: %(message)s'
    formatter = logging.Formatter(fmt)
    handler = RotatingFileHandler(log_file, maxBytes = 100*1024*1024, backupCount = 5)
    handler.setFormatter(formatter)   
    logger = logging.getLogger('main')  
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)  


