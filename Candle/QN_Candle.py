# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback, datetime, sched, time, pymongo, collections, multiprocessing, time
import setup
sys.path.append(os.getcwd() + r'\Candle')
import QN_CorrectCandle
sys.path.append(os.getcwd() + r'\..\Common')
import mssqlAPI, mongoAPI, logFile, common, dbServer


def cleanInvalidData(mongoList):
  '''
  删除Mongo库中历史行情无效数据
  '''
  tableNameList = ['CandleDay', 'CandleWeek', 'CandleMonth', 'KDayData', 'KWeeklyData', 'KMonthData']
  for mongo in mongoList:
    mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
    for tableName in tableNameList:
      try:
        # 开盘价、收盘价、最高价、最低价，有一个为0，则判定为需要删除的无效记录。
        mongoHandle.remove(tableName, 'data', {'$or':[{'OP':0}, {'CP':0}, {'HP':0}, {'LP':0}]})
      except:
        print traceback.format_exc()

  return None


def createIndex(mongoList):
  '''
  创建Mongo库中历史行情索引
  '''
  tableNameList = ['CandleDay', 'CandleWeek', 'CandleMonth', 'KDayData', 'KWeeklyData', 'KMonthData']
  for mongo in mongoList:
    mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
    for tableName in tableNameList:
      mongoHandle.createIndex(tableName, 'data', [('_id.IC', pymongo.ASCENDING)])
      mongoHandle.createIndex(tableName, 'data', [('TO', pymongo.DESCENDING)])

  return None


def correctCandle(mssqlDict, mongoList):
  '''
  修复Mongo库中历史行情
  '''
  correctCandleHandle = QN_CorrectCandle.CorrectCandle(mssqlDict, mongoList)
  monitorProcessDict = dict()
  # 更新MSSQL库历史行情信息
  correctCandleHandle.updateMssql()
  # 查询SQL
  monitorProcessDict['QueryMssql'] = correctCandleHandle.queryMssqlProc()
  # 指标计算
  monitorProcessDict['CalculateData'] = correctCandleHandle.calculateDataProc()
  # 更新Mongo
  monitorProcessDict['UpdateMongo'] = correctCandleHandle.updateMongoProc()

  # 已终止进程数量
  terminateNum = 0
  while True:
    #移除已终止的进程
    for processName in monitorProcessDict:
      for process in monitorProcessDict[processName]:
        if (process.is_alive() == False):
          monitorProcessDict[processName].remove(process)
    #检查计算，SQL写入，MongoDB写入进程，如进程意外终止，则重新启动进程
    while (len(monitorProcessDict['CalculateData']) < setup.setupDict['ProcessNum']['CalculateData']):
      process = multiprocessing.Process(target=correctCandleHandle.calculateData, args=(correctCandleHandle.querySqlQueue, correctCandleHandle.updateMongoQueue))
      process.start()
      monitorProcessDict['CalculateData'].append(process)
    while (len(monitorProcessDict['UpdateMongo']) < setup.setupDict['ProcessNum']['UpdateMongo']):
      process = multiprocessing.Process(target=correctCandleHandle.updateMongo, args=())
      process.start()
      monitorProcessDict['UpdateMongo'].append(process)
    # 查询结果信息
    for processName in monitorProcessDict:
      print processName, len(monitorProcessDict[processName])
    print 'queryMssqlQueue:',correctCandleHandle.queryMssqlQueue.qsize()
    print 'mongoQueue:',correctCandleHandle.updateMongoQueue.qsize()
    if ((len(monitorProcessDict['QueryMssql']) == 0) and
        (correctCandleHandle.queryMssqlQueue.qsize() == 0) and
        (correctCandleHandle.updateMongoQueue.qsize() == 0)):
      terminateNum += 1
    #终止判断
    if (terminateNum >= 10):
      #关闭所有进程
      for processName in monitorProcessDict:
        for process in monitorProcessDict[processName]:
          if (process.is_alive() == True):
            process.terminate()
      del correctCandleHandle
      break
    time.sleep(10)

  return None


def cleanOverdueData(mongoList, keepNum = 61):
  '''
  清理过期历史行情数据
  '''
  tableNameList = ['CandleDay', 'CandleWeek', 'CandleMonth']
  for mongo in mongoList:
    mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
    for tableName in tableNameList:
      records = mongoHandle.find(tableName, 'data', None, [('_id', pymongo.DESCENDING)])
      if records is not None:
        innerCode = None
        for record in records:
          if (record['_id']['IC'] != innerCode):
            innerCode = record['_id']['IC']
            numOfInnerCode = 1
          else:
            numOfInnerCode += 1

          if (numOfInnerCode > keepNum):
            keyDict = collections.OrderedDict()
            keyDict['IC'] = record['_id']['IC']
            if (tableName == 'CandleDay'):
              keyDict['TO'] = record['_id']['TO']
            elif (tableName == 'CandleWeek'):
              keyDict['YR'] = record['_id']['YR']
              keyDict['WK'] = record['_id']['WK']
            elif (tableName == 'CandleMonth'):
              keyDict['YR'] = record['_id']['YR']
              keyDict['MN'] = record['_id']['MN']
            mongoHandle.remove(tableName, 'data', {'_id':keyDict})

  return None


def candle(mssqlDict, mongoList):
  '''
  历史行情更新操作
  '''
  startTime = datetime.datetime.now()
  # 清理Mongo库历史行情中存在的异常数据
  cleanInvalidData(mongoList)
  # 创建Mongo库历史行情索引
  createIndex(mongoList)
  # 更新Mongo库历史行情信息
  correctCandle(mssqlDict, mongoList)
  # 删除Mongo库历史行情中多余的数据
  cleanOverdueData(mongoList)

  finalTime = datetime.datetime.now()
  deltaTime = finalTime - startTime
  totalTime = deltaTime.total_seconds()
  totalHour = totalTime // 3600
  totalMin = (totalTime % 3600) // 60
  totalSec = totalTime % 60
  print("Total time: %d(h)%d(m)%d(s)" % (int(totalHour), int(totalMin), int(totalSec)))
  logFileHandle = logFile.LogFile(name = 'Candle')
  logFileHandle.logInfo('Correct all security info succeed, total time: ' + str(int(totalHour)) + '(h)' + str(int(totalMin)) + '(m)' + str(int(totalSec)) + '(s)')

  return None


def startCandle(mssqlDict, mongoList, atOnce = False):
  '''
  历史行情更新进程创建
  '''
  if atOnce:
    candle(mssqlDict, mongoList)

  while True:
    try:
      # Get current datetime
      startTime = datetime.datetime.now()
      timetuple = startTime.timetuple()
      executeTime = datetime.datetime(
        year = timetuple.tm_year,
        month = timetuple.tm_mon,
        day = timetuple.tm_mday,
        hour = setup.setupDict['Candle']['Schedule']['Hour'],
        minute = setup.setupDict['Candle']['Schedule']['Minute'],
        second = setup.setupDict['Candle']['Schedule']['Second'])
      # Execute function next day this time
      if (executeTime < startTime):
        executeTime += datetime.timedelta(days=1)
        print 'Candle next execute time: %s' % executeTime.strftime('%Y-%m-%d %X')
      # Get delta time within next execute time and date time now.
      deltaTime = executeTime - startTime
      scheduleHandle = sched.scheduler(time.time, time.sleep)
      scheduleHandle.enter(deltaTime.total_seconds(), 1, candle, (mssqlDict, mongoList))
      scheduleHandle.run()
    except:
      print traceback.format_exc()


if __name__ == '__main__':
  mssqlDict = dbServer.mssqlDbServer['46']['LAN']
  mongoList = [dbServer.mongoDbServer['22']['LAN'], dbServer.mongoDbServer['23']['LAN']]
  startCandle(mssqlDict, mongoList, atOnce=True)
