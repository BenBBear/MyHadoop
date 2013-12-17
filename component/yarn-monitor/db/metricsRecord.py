#!python
# coding=utf8
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()
from sqlalchemy import Column, Integer, String , SmallInteger,BigInteger

#metricsRecord

class metricsRecord(Base):
    __tablename__ = 'metrics'
    
    recordTime = Column(Integer, primary_key=True)
    
    #应用状况
    appsCompleted = Column(Integer)
    appsPending = Column(SmallInteger)
    appsRunning = Column(SmallInteger)
    appsFailed = Column(SmallInteger)
    appsKilled = Column(SmallInteger)
    
    #资源分配
    totalMB = Column(Integer)
    allocatedMB = Column(Integer)
    containersAllocated = Column(Integer)
    containersReserved = Column(Integer)
    containersPending = Column(Integer)
    
    #结点相关
    totalNodes = Column(SmallInteger)
    activeNodes = Column(SmallInteger)
    
    def __init__(self,recordTime):
        self.recordTime = recordTime
        
    def set(self,key,value):
        setattr(self,key,value)
        