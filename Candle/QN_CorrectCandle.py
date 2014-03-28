# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback, multiprocessing
import setup
sys.path.append(os.getcwd() + r'\Candle')
import QN_constant, QN_QueryMssql, QN_CalculateData, QN_UpdateMongo
sys.path.append(os.getcwd() + r'\..\Common')
import logFile, mssqlAPI


class CorrectCandle(object):
  '''
  历史行情修复
  '''
  def __init__(self, mssqlDict, mongoList):
    self.mssqlDict = mssqlDict
    self.mongoList = mongoList
    # LogFile句柄
    self.logHandle = logFile.LogFile(name = 'Candle')
    # 创建消息队列
    self.queryMssqlQueue = multiprocessing.Queue(setup.setupDict['QueueVolume']['QueryMssql'])
    self.updateSqlQueue = multiprocessing.Queue(setup.setupDict['QueueVolume']['UpdateMssql'])
    self.updateMongoQueue = multiprocessing.Queue(setup.setupDict['QueueVolume']['UpdateMongo'])

    return None


  def __del__(self):
    # LogFile句柄
    if self.logHandle is not None:
      del self.logHandle

    return None


  def updateMssql(self):
    '''
    执行存储过程更新MSSQL数据
    '''
    mssqlHandle = mssqlAPI.MssqlAPI(
      server = self.mssqlDict['SERVER'],
      db = self.mssqlDict['DB'],
      user = self.mssqlDict['USER'],
      pwd = self.mssqlDict['PWD'])
    try:
      mssqlHandle.sqlExecuteProc(name='usp_UpdateFrom251', ())
      mssqlHandle.sqlCommit()
    except:
      self.logHandle.logInfo(str(traceback.format_exc()))
      print traceback.format_exc()


  def queryMssql(self, db):
    '''
    SQL查询操作
    '''
    queryMssqlHandle = QN_QueryMssql.QueryMssql(self.mssqlDict, self.queryMssqlQueue)
    # 分代码查询，增加与SQL交互，减少内存占用率，查询效率会略有下降。
    stockCodeList = queryMssqlHandle.getStockCodeList()
    if (stockCodeList is not None):
      for stockCode in stockCodeList:
        queryMssqlHandle.getSpecifyData(db, [stockCode])

    return None


  def queryMssqlProc(self):
    '''
    SQL查询启动进程
    '''
    queryMssqlProcessList = list()
    for db in QN_constant.dbDict:
      process = multiprocessing.Process(target=self.queryMssql, args=(db, ))
      process.start()
      queryMssqlProcessList.append(process)

    return queryMssqlProcessList


  def calculateData(self):
    '''
    计算操作
    '''
    calculateDataHandle = QN_CalculateData.CalculateData(self.updateMongoQueue)
    for msg in iter(self.querySqlQueue.get, None):
      calculateDataHandle.calculateData(msg['DB'], msg['MSG'])
    '''
    except KeyError:
      calculateDataHandle.calculateData(msg['DB'], msg['MSG'], list(), extensionQueue)
    '''
    return None


  def calculateDataProc(self, processNum = setup.setupDict['ProcessNum']['CalculateData']):
    '''
    计算启动进程
    '''
    calculateDataProcessList = list()
    for num in range(0, processNum):
      process = multiprocessing.Process(target=self.calculateData, args=(self.querySqlQueue, self.updateMongoQueue))
      process.start()
      calculateDataProcessList.append(process)

    return calculateDataProcessList


  def updateMongo(self):
    '''
    更新Mongo操作
    '''
    updateMongoHandle = QN_UpdateMongo.UpdateMongo(self.mongoList)
    for msg in iter(self.updateMongoQueue.get, None):
      updateMongoHandle.updateMongo(msg)

    return None


  def updateMongoProc(self, processNum = setup.setupDict['ProcessNum']['UpdateMongo']):
    '''
    更新Mongo启动进程
    '''
    updateMongoProcessList = list()
    for num in range(0, processNum):
      process = multiprocessing.Process(target=self.updateMongo, args=())
      process.start()
      updateMongoProcessList.append(process)

    return updateMongoProcessList
