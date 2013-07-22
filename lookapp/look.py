#!/usr/bin/env python
# coding=utf8

# author: zhaigy@ucweb.com
# data  : 2013-07-17

import os
import sys
import json
import time

DATA="./data"

zero = time.localtime()
zero = time.mktime((zero.tm_year, zero.tm_mon, zero.tm_mday,0,0,0,0,0,0))
zero = int(zero)
blank100 = "                                                                                                    "
point100 = "||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||"
#print zero
#print time.strftime("%Y%m%d%H%M%S",time.localtime(zero))
#sys.exit(0)

full = zero * 1000
apps = {}
for day in os.listdir(DATA):
  #print day
  for f in os.listdir("%s/%s" % (DATA, day)):
    #print f
    j = json.load(open("%s/%s/%s" % (DATA, day, f),"r"))
    #print j
    if not j['apps']: continue
    for app in j['apps']['app']:
      id = app['id']
      if not id in apps or (apps[id]['elapsedTime'] <= app['elapsedTime']):
        apps[id] = app
      lastTime = app['startedTime'] + app['elapsedTime']
      if lastTime > full:
        full = lastTime
      #{
      #    "user" : "user1",
      #    "id" : "application_1326815542473_0001",
      #    "progress" : 100,
      #    "name" : "word count",
      #    "startedTime" : 1326815573334,
      #    "elapsedTime" : 25196,
      #    "queue" : "default"
      # },

full = ((full / 1000) - zero) / 10
appids = apps.keys()
appids = sorted(appids)
for id in appids:
  # 20%s                 |123456123456123456123456123456123456123456123456123456123456123456123456(12*6)
  #application_1373357024953_124150|
  app = apps[id]
  start = app['startedTime']
  start = start / 1000
  start = start - zero
  start_idx = start / 10
  pre_blank = ""
  if start_idx>0:
    pre_blank = blank100[:start_idx]
  start_point = "|"
  elapsed = app['elapsedTime']
  elapsed = elapsed / 1000 / 10
  if elapsed > 100:
    elapsed = 100
  elapsed_point = point100[:elapsed] 
  print "%20s|%s%s%s" % (id[12:], pre_blank, start_point, elapsed_point)
