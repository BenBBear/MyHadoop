#!python
# coding=utf8

import logging
import urllib2
from conf import config
import json
from lib import log
import threading
log.initLogger("util.log")

    
def jobidToAppid(jobid):
        return "application" + jobid[3:]
    
def appidToJobid(appid):
    return "job" + appid[11:]

def getHttp(url , timeout = 10):
    try:
        req = urllib2.Request(url)
        response = urllib2.urlopen(req ,None, timeout)
        html = response.read()
        return html
    except:
        # print "get exception while getting "+url;
        logger = logging.getLogger("main")
        logger.exception("get http error:"+url)
    return None

def getHttpJson(url , timeout = 10):
    print url
    html = getHttp(url, timeout)
    try:
        if html:
            return json.loads(html)
        else:
            return None
    except:
        return None
    return None

# def worker(a_tid,a_account): 
#     global g_mutex 
#     print "Str " , a_tid, datetime.datetime.now() 
#     for i in range(1000000): 
#         #g_mutex.acquire() 
#         a_account.deposite(1) 
#         #g_mutex.release() 
#     print "End " , a_tid , datetime.datetime.now() 
#     
# def getMulitHttpJson(urls,poolsize,timeout = 1):
#     thread_pool = [] 
#     #init mutex 
#     g_mutex = threading.Lock() 
#     # init thread items 
#     acc = Account(100) 
#     for i in range(10): 
#         th = threading.Thread(target=worker,args=(i,acc) ) ; 
#         thread_pool.append(th) 
#          
#     # start threads one by one         
#     for i in range(10): 
#         thread_pool[i].start() 
#      
#     #collect all threads 
#     for i in range(10): 
#         threading.Thread.join(thread_pool[i])
#         
#     pool = ThreadPool(poolsize) 
#     for (appid,url) in urls:
        


#转化为10为的长度,且转化为开头的整10分钟
def getIntervalTime(time):
    if time > 1000000000000L:
        time /= 1000
    return (int(time)/config.collect_interval)*config.collect_interval
    
#转化为10为的长度,且转化为开头的整10分钟
def getSecondTime(time):
    if time > 1000000000000L:
        time /= 1000
    return time

if __name__ == "__main__":
    print getHttp("http://mob616:50088/ws/v1/cluster/apps?state=RUNNING")