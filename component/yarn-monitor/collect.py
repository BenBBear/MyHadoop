#!python
# coding=utf8

from db import database
from lib import util
from conf import config
import time
from db.applicationRecord import applicationRecord
from db.nmRecord import nmRecord
from db.rmRecord import rmRecord
from db.metricsRecord import metricsRecord
from lib import log
import json
import urllib2
import re
import logging
import os
# 采集数据
# 获取最近10分钟完成的任务
# os.makedirs(config.logpath)
# logging.basicConfig(filename = os.path.join(config.logpath, 'collect.txt'), level = logging.DEBUG,
#                     format = '%(asctime)s - %(levelname)s: %(message)s')
log.initLogger('collect.log')
logger = logging.getLogger('main')

class collector:
    #默认运行上个10分钟的数据
    def __init__(self, beginTime=( time.time()-config.collect_interval) ):
        self.interval = config.collect_interval
        self.recordTime = util.getIntervalTime(beginTime)
        msg = "begin to run at"+ time.strftime('%Y-%m-%d %A %X',time.localtime(self.recordTime))
        logger.info(msg)
        print msg
        self.rmList = {}
        self.nmList = {}
        self.appList = {} 
    
    def collectMetrics(self):
        #获取当前集群的状态
        metrics = self.getMetrics()
        meRecord =  metricsRecord(self.recordTime);
        recordKey=["appsCompleted","appsPending","appsRunning",
                   "appsFailed","appsKilled","totalMB","allocatedMB",
                   "containersAllocated","containersReserved",
                   "containersPending","totalNodes","activeNodes"]
        print metrics
        for key in recordKey:
            print metrics['clusterMetrics'][key]
            meRecord.set(key,metrics['clusterMetrics'][key])
        session = database.getSession()
        session.merge(meRecord)
        session.commit()
         
    def collectApp(self):
                
        #获取所有的过去时段完成的app的列表
        apps = self.getAppList()
        if not apps or not apps["apps"]:
            logger.info("no appid match")
            return
        
        startCollectTime = time.time()
        #轮询app列表，获取每个app的详细信息
        for app in apps["apps"]["app"]:
            startTime = time.time()
            appid =  app["id"]
            if app['state'] == 'FINISHED':
                try:                
                    jobid = util.appidToJobid(appid)
                    jobHistory = self.getJobHistory(jobid)
                    if jobHistory: 
                        jobCounter = self.getJobCounter(jobid)
                        jobTasks = self.getJobAllTask(jobid)
                        self.updateWithAppid(app,jobHistory,jobCounter)
                    else:
                        logger.info("find some app run success but no history file:"+appid)
                except:
                    logger.exception("get error while doing app "+appid)
                endTime = time.time()
            else:
                self.updateWithNotSuccAppid(app)
                
            logger.info("getting appid: %s using %d ms" % (appid, (endTime - startTime)*1000))
            
        endCollectTime = time.time()
        logger.info("using %d ms to collect the data" % ((endCollectTime - startCollectTime)*1000) )
        
        startFlushTime = time.time()
        
        #提交数据
        session = database.getSession()
        for (appid,appRecordValue) in self.appList.items():
            session.merge(appRecordValue)
        session.commit()
        logger.info("push %d appRecord into table" % (len(self.appList)))
        
        for (key,nmRecordValue) in self.nmList.items():
            session.merge(nmRecordValue)
        session.commit()
        logger.info("push %d nmRecord into table" % (len(self.nmList)))
        
        for (key,rmRecordValue) in self.rmList.items():
            session.merge(rmRecordValue)
        session.commit()
        logger.info("push %d rmRecord into table" % (len(self.rmList)))
        endFlushTime = time.time()
        
        logger.info("using %d ms to push to the db" % ((endFlushTime - startFlushTime)*1000))
            
        
    def getJobHistory(self,jobid):
        url = ("http://%s:%s/ws/v1/history/mapreduce/jobs/%s" 
               % (config.hshost,config.hsport,jobid))
        return util.getHttpJson(url)
    
    def getJobAllTask(self,jobid):
        url = ("http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks"
               % (config.hshost,config.hsport,jobid))
        tasks = util.getHttpJson(url)
        if not tasks or not tasks.has_key('tasks') or not tasks['tasks'].has_key('task') :
            return
        for task in tasks['tasks']['task']:
            taskId = task['id']
            url = ("http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s/attempts"
               % (config.hshost,config.hsport,jobid,taskId))
            attempts = util.getHttpJson(url)
            if not attempts or not attempts.has_key('taskAttempts') \
                or not attempts['taskAttempts'].has_key('taskAttempt') :
                return
            for attempt in attempts['taskAttempts']['taskAttempt']:
                attemptId = attempt['id']
                url = ("http://%s:%s/ws/v1/history/mapreduce/jobs/%s/tasks/%s/attempts/%s/counters"
                   % (config.hshost,config.hsport,jobid,taskId,attemptId))
                attemptCounter = util.getHttpJson(url)
                self.updateWithAttempt(attempt,attemptCounter)

                    
    def getNodeFromAddress(self,address):
        split = address.find(":")
        return address[0:split]
    
    def getJobCounter(self,jobid):
        """
        从appid的对应的counter的网页上截取信息.从restapi获取的不全,缺少data-local等的信息
        """
        url = ("http://%s:%s/jobhistory/jobcounters/%s" 
               % (config.hshost,config.hsport,jobid))
        html = util.getHttp(url)
        if not html:
            return None
        keys = ["DATA_LOCAL_MAPS","RACK_LOCAL_MAPS",
                "FILE_BYTES_READ","FILE_BYTES_WRITTEN",
                "HDFS_BYTES_READ","HDFS_BYTES_WRITTEN"]
        counters = {}
        for key in keys:
            counters[key] = self.getCounterFromHtml(html,key)
        return counters
    
    def getCounterFromHtml(self,html,key):
        pattern = re.compile('<td title="'+key+'">.*?<td>.*?(\d+).*?<td>.*?(\d+).*?<td>.*?(\d+).*?<tr>',re.S)
        match = pattern.search(html)
        if match:
            # 使用Match获得分组信息
            return {"map":match.group(1),"reduce":match.group(2),"total": match.group(3) }
        else:
            return {"map":0,"reduce":0,"total":0}
    
    def getMetrics(self):    
        url = ("http://%s:%s/ws/v1/cluster/metrics" % (config.rmhost,config.rmport))
        return util.getHttpJson(url)
    
    def getAppList(self):
        url = ("http://%s:%s/ws/v1/cluster/apps?finishedTimeBegin=%d&finishedTimeEnd=%d" 
            % (config.rmhost,config.rmport,(self.recordTime*1000),(self.recordTime+self.interval)*1000))
        return util.getHttpJson(url)
        
    #以下是用于程序内计数的函数
    def updateWithAttempt(self,attempt,attemptCounter):
        #update nm's containerNum , mapNum , reduceNum
        node = self.getNodeFromAddress(attempt['nodeHttpAddress'])
        happenTime = util.getIntervalTime(attempt['startTime'])
        rm = self.getRm(self.recordTime,happenTime)
         
        nm = self.getNm(node,self.recordTime, happenTime)
        #*********************************
        nm.inc("containerNum",1)
        
        if attempt['type'] == 'MAP':
            rm.inc("mapNum",1)
            nm.inc("mapNum",1)
            rm.inc("mapTime",attempt['elapsedTime'])
            nm.inc("mapTime",attempt['elapsedTime'])
            if attempt['state'] != "SUCCEEDED":
                rm.inc("failMap",1)
                nm.inc("failMap",1)
        elif attempt['type'] == 'REDUCE':
            rm.inc("reduceNum",1)
            nm.inc("reduceNum",1)
            rm.inc("reduceTime",attempt['elapsedTime'])
            nm.inc("reduceTime",attempt['elapsedTime'])
            if attempt['state'] != "SUCCEEDED":
                rm.inc("failReduce",1)
                nm.inc("failReduce",1)
        #*********************************
        if not attemptCounter or not attemptCounter.has_key('jobTaskAttemptCounters') \
            or not attemptCounter['jobTaskAttemptCounters'].has_key('taskAttemptCounterGroup'):
            return 
        for taskAttemptCounterGroup in attemptCounter['jobTaskAttemptCounters']['taskAttemptCounterGroup']:
            if taskAttemptCounterGroup['counterGroupName'] == "org.apache.hadoop.mapreduce.FileSystemCounter":
                for counter in taskAttemptCounterGroup['counter']:
                    if counter['name'] == 'FILE_BYTES_READ':
                        rm.inc("fileRead",counter["value"])
                        nm.inc("fileRead",counter["value"])
                    elif counter['name'] == 'FILE_BYTES_WRITTEN':
                        rm.inc("fileWrite",counter["value"])
                        nm.inc("fileWrite",counter["value"])
                    elif counter['name'] == 'HDFS_BYTES_READ':
                        rm.inc("hdfsRead",counter["value"])
                        nm.inc("hdfsRead",counter["value"])
                    elif counter['name'] == 'HDFS_BYTES_WRITTEN':
                        rm.inc("hdfsWrite",counter["value"])
                        nm.inc("hdfsWrite",counter["value"])
            else:
                continue
    def updateWithNotSuccAppid(self,app):
        appHappenTime = util.getIntervalTime(app['startedTime'])
        rm = self.getRm(self.recordTime,appHappenTime)
        
        rm.inc("appNum",1)
        if app['stats'] == 'KILLED':
            rm.inc("killedApp",1)
        elif app['stats'] == 'FAILED':
            rm.inc("failedApp",1)
        
    def updateWithAppid(self,app,jobHistory,jobCounter):
        #update nm and rm
        amNode = self.getNodeFromAddress(app['amHostHttpAddress'])
        appHappenTime = util.getIntervalTime(app['startedTime'])
        nm = self.getNm(amNode,self.recordTime, appHappenTime)
        rm = self.getRm(self.recordTime,appHappenTime)
        
        nm.inc("containerNum",1);
        nm.inc("amNum",1);

        rm.inc("appNum",1)
        rm.inc("finishedApp",1)
        
        if app['finalStatus'] != "SUCCEEDED":
            rm.inc("notSuccApp",1)
        #end update
        appid =  app["id"]
        appRecord = self.getAppidRecord(appid)
        keyFromApp = ["user","name","queue","startedTime","finishedTime","state","finalStatus"]
        for key in keyFromApp:
            if key == "startedTime" or key == "finishedTime":
                appRecord.set(key,util.getSecondTime(app[key]))
            else:    
                appRecord.set(key,app[key])
        #todo
        appRecord.set("attemptNumber",1)
        keyFromHistory = ["mapsTotal","mapsCompleted","successfulMapAttempts",
                          "killedMapAttempts","failedMapAttempts","avgMapTime",
                          "reducesTotal","reducesCompleted","successfulReduceAttempts",
                          "killedReduceAttempts","failedReduceAttempts","avgReduceTime"]
        if jobHistory.has_key('job'):
            for key in keyFromHistory:
                appRecord.set(key,jobHistory['job'][key])
        #TODO "localMap","rackMap"
        keyMapFromCounters = {"DATA_LOCAL_MAPS":"localMap","RACK_LOCAL_MAPS":"rackMap",
                "FILE_BYTES_READ":"fileRead","FILE_BYTES_WRITTEN":"fileWrite",
                "HDFS_BYTES_READ":"hdfsRead","HDFS_BYTES_WRITTEN":"hdfsWrite"}
        if jobCounter:
            for (key,value) in keyMapFromCounters.items():
                appRecord.set(value,jobCounter[key]['total'])

    def getNm(self,node,recordTime,happenTime):
        key = node + str(recordTime) +str(happenTime)
        if not self.nmList.has_key(key):
            self.nmList[key] =  nmRecord(node,recordTime,happenTime)
        return self.nmList[key]
    
    def getAppidRecord(self,appid):
        if not self.appList.has_key(appid):
            self.appList[appid] = applicationRecord(appid)
        return self.appList[appid]

    def getRm(self,recordTime,happenTime):
        key = str(recordTime) +str(happenTime)
        if not self.rmList.has_key(key):
            self.rmList[key] =  rmRecord(recordTime,happenTime)
        return self.rmList[key]

    def upateNmData(self,node,recordTime,key,value):
        self.nm[node][recordTime][key]+=value
    
if __name__ == "__main__":
    coll = collector()
    coll.collectMetrics()
    #为了防止有些任务未成功收尾，延迟2分钟执行
    time.sleep(120)
    coll.collectApp()    
