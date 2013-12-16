#!/usr/bin/env python
# coding=utf8

from conf import config
import sqlite3

class database:
    def __init__(self,path=config.sqlitepath):
        self.conn = sqlite3.connect(config.sqlitepath) 
        cursor = self.conn.cursor()
        #初始化
        cursor.execute('''CREATE TABLE IF NOT EXISTS result (name text, time int)''');
        
    def getCursor(self):
        return self.conn.cursor();
    
    def commit(self):
        self.conn.commit();
        
    def close(self):
        self.conn.close();

if __name__ == '__main__':
    conn = sqlite3.connect(config.sqlitepath) 
    cursor = conn.cursor()
    #初始化
    cursor.execute('select count(appid) , sum(mapsTotal) , sum(mapsCompleted) , sum(successfulMapAttempts) , sum(killedMapAttempts) , sum(failedMapAttempts) , sum(avgMapTime) , sum(localMap) , sum(rackMap) , sum(reducesTotal) , sum(reducesCompleted) , sum(successfulReduceAttempts) , sum(killedReduceAttempts) , sum(failedReduceAttempts) , sum(avgReduceTime) , sum(fileRead) , sum(fileWrite) , sum(hdfsRead) , sum(hdfsWrite) from app where 1 order by appid ');
    print cursor.fetchone()
    print "debug"
    
    

