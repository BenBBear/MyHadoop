#!/usr/bin/env python
# coding=utf8

# nodedata:存放look.sh采集的数据目录
nodedata = "/home/qiujw/MyHadoop/lookapp/nodedata"

# log 's path
logpath = "d:\webpython\logs"

# sqllite db 's path
sqlitepath = "d:\sqllite.db"
#!python
# coding=utf8

# node manager's port
nmport = 59842

# resource manager's host and port
rmhost = "vdc21"
rmport = "50088"

# rmhost="hadoop2"
# rmport="59088"

# resource manager's host and port
hshost = "hadoop2"
hsport = "59888"

# hshost="hadoop1"
# hsport="59888"

# collect run interval
collect_interval = 600

hosts = ["hadoop2","hadoop3","hadoop4","hadoop5"]
# hosts = ["kpi11", "kpi12", "kpi13", "kpi14", "kpi15", "kpi17",
#        "kpi18", "kpi19", "kpi25", "kpi26", "kpi27", "kpi28", 
#        "kpi39", "kpi40", "kpi41", "kpi42", "kpi43", "kpi44", 
#        "kpi45", "kpi46", "kpi47", "kpi48", "kpi49", "kpi50", 
#        "kpi51", "kpi52"]
