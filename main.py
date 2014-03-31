# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''

import time, multiprocessing, sys, os, traceback
import setup
sys.path.append(os.getcwd() + r'\Candle')
import QN_Candle
sys.path.append(os.getcwd() + r'\CreateIndex')
import QN_CreateIndex
sys.path.append(os.getcwd() + r'\RestoreClose')
import QN_RestoreClose
sys.path.append(os.getcwd() + r'\RightInfo')
import QN_RightInfo
sys.path.append(os.getcwd() + r'\StockInfo')
import QN_StockInfo
sys.path.append(os.getcwd() + r'\..\Common')
import logFile, dbServer


def startCandleProc(mssql, mongoList, atOnce = setup.setupDict['Candle']['AtOnce']):
  '''
  历史K线作业进程启动函数
  '''
  print 'startCandleProc >>'
  process = multiprocessing.Process(target = QN_Candle.startCandle, args = (mssql, mongoList, atOnce))
  process.start()

  return process


def startCreateIndexProc(mssql, mongoList, atOnce = setup.setupDict['CreateIndex']['AtOnce']):
  '''
  Mongo索引创建作业进程启动函数
  '''
  print 'startCreateIndexProc >>'
  process = multiprocessing.Process(target = QN_CreateIndex.startCreateIndex, args = (mongoList, atOnce))
  process.start()

  return process


def startRestoreCloseProc(mssql, mongoList, atOnce = setup.setupDict['RestoreClose']['AtOnce']):
  '''
  复权收盘价作业进程启动函数
  '''
  print 'startRestoreCloseProc >>'
  process = multiprocessing.Process(target = QN_RestoreClose.startRestoreClose, args = (mssql, mongoList, atOnce))
  process.start()

  return process


def startRightInfoProc(mssql, mongoList, atOnce = setup.setupDict['RightInfo']['AtOnce']):
  '''
  复权信息作业进程启动函数
  '''
  print 'startRightInfoProc >>'
  process = multiprocessing.Process(target = QN_RightInfo.startRightInfo, args = (mssql, mongoList, atOnce))
  process.start()

  return process


def startStockInfoProc(mssql, mongoList, atOnce = setup.setupDict['StockInfo']['AtOnce']):
  '''
  股票信息作业进程启动函数
  '''
  print 'startStockInfoProc >>'
  process = multiprocessing.Process(target = QN_StockInfo.startStockInfo, args = (mssql, mongoList, atOnce))
  process.start()

  return process


startFuncDict = {
  'CreateIndex'   :startCreateIndexProc,
  'RestoreClose'  :startRestoreCloseProc,
  'RightInfo'     :startRightInfoProc,
  'StockInfo'     :startStockInfoProc}


def startProc(mssqlDict, mongoList):
  '''
  各任务进程启动指令
  '''
  processDict = dict()
  for key in startFuncDict:
    if (setup.setupDict[key]['Valid']):
      processDict[key] = startFuncDict[key](mssql = mssqlDict, mongoList = mongoList, atOnce = setup.setupDict[key]['AtOnce'])

  return processDict


if __name__ == '__main__':
  '''
  千牛行情Python任务入口函数
  '''
  try:
    # 日志句柄
    logHandle = logFile.LogFile(name = 'QN_Quotation')
    mssqlDict = dbServer.mssqlDbServer['46']['LAN']
    mongoList = [dbServer.mongoDbServer['22']['LAN'], dbServer.mongoDbServer['23']['LAN']]
    # 启动各任务进程
    processDict = startProc(mssqlDict, mongoList)
    # 启动历史行情计算
    if (setup.setupDict['Candle']['Valid']):
      QN_Candle.startCandle(mssqlDict = mssqlDict, mongoList = mongoList, atOnce = setup.setupDict['Candle']['AtOnce'])
    '''
    while True:
      time.sleep(5)
      for key in startFuncDict:
        if (processDict[key].is_alive()):
          logHandle.logInfo(key + ' process is alive.')
          print key + 'process is alive.'
    '''
  except:
    print traceback.format_exc()
