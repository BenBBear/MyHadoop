#!/usr/bin/env python
# coding=utf8

# author: qiujw@ucweb.com
# data  : 2013-07-22

import os
import sys
import json
import time
import re
import operator 

nodedata = "/home/qiujw/MyHadoop/lookapp/nodedata"
regex = ur"[0-9]{13}_[0-9]{4,5}"

def node_container_data(type,date,start,end,top):
    #初始化xaxis
    xaxis = []
    for hour in range(start,end+1):
        for min in range(60): 
            time = "%02d%02d" % (hour,min)
            xaxis.append(time)

    apps = {}
    nodes = {}
    appcache = {}
    containercache = {}

    temp = os.path.join(nodedata,date);
    if not os.path.isfile(temp):
        raise Exception ,'no data dir '+temp

    #结点循环
    for node in os.listdir(temp):
      #小时循环
      for hour in range(start,end+1):
          #分钟循环
          for min in range(60):
              time = "%02d%02d" % (hour,min)
              filename = "%02d%02d.log" % (hour,min)
              path = os.path.join(temp,node,filename)
              if not os.path.isfile(path): continue;
              j = json.load(open(path,"r"))
              if not j['containers']:continue
              for c in j['containers']['container']:
                  cid = c['id']
                  match = re.search(regex,cid);
                  if match :
                      appid = match.group()
                  else : 
                      appid = "-1" 
                  cacheapp(appcache,time,appid)
                  mapinc(containercache,time,1)
                  inc(apps,appid,time,1)
                  inc(nodes,node,time,1)

    if type == "app":
        map = apps
    else :
        map = nodes
    cmap = combine(map,top)
    appseries = get_appseries(appcache,containercache,start,end)
    return (xaxis,dict_to_series(cmap,xaxis),appseries)

def inc(map,key,time,number):
    if not map.has_key(key):
        map[key]={}
    t = map[key].get(time,0)
    t += number;
    map[key][time] = t

def mapinc(map,key,number):
    t = map.get(key,0)
    t += number
    map[key] = t

def cacheapp(appcache,time,appid):
    if not appcache.has_key(time):
         appcache[time] = set()
    appcache[time].add(appid)

def dict_to_series(map,xaxis):
    series=[]
    for key in map:
        temp = {"name":key,"data":[]}
        for time in xaxis:
            t = map[key].get(time,0);
            temp["data"].append(t);
        series.append(temp)
    return series;

def get_appseries(appcache,containercache,start,end):
    appseries=[{"name":"apps","data":[]},{"name":"container","data":[]}]
    appdata=[]
    containerdata=[]
    for hour in range(start,end+1):
      #分钟循环
        for min in range(60):
            time = "%02d%02d" % (hour,min)
            if appcache.has_key(time):
                  appdata.append( len(appcache[time]) )
            else :    
                  appdata.append( 0 )
            if containercache.has_key(time):
                  containerdata.append( containercache[time] );
            else :
                  containerdata.append( 0 );
    appseries=[{"name":"apps","data":appdata},{"name":"container","data":containerdata}]
    return appseries


#将数组按照占用的数量排序，只取前10，其余的归入other
def combine(map,top):
    cmap={}
    sum={}
    for key in map:
        t = 0;
        for (time,number) in map[key].items():
            t += int(number)
        sum[key]=t;
    smap = sorted(sum.iteritems(), key=operator.itemgetter(1),reverse=True)
    i = 0;
    for (skey,svalue) in smap:
        if i < top:
          cmap[skey] = map[skey];  
        else:
            print map[skey]
            for (k,v) in map[skey].items():
                inc(cmap,"other",k,v)  
        i +=1
    return cmap;
