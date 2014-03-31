# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback, pymongo, datetime, time, sched
import setup
sys.path.append(os.getcwd() + r'\..\Common')
import mongoAPI, logFile, dbServer


def createCandleDayIndex(mongoHandle):
  '''
  Create index into mongoDB 'CandleDay.data'
  '''
  tableName = 'CandleDay'
  collectionName = 'data'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('TO', pymongo.DESCENDING)])

  return None


def createCandleWeekIndex(mongoHandle):
  '''
  Create index into mongoDB 'CandleWeek.data'
  '''
  tableName = 'CandleWeek'
  collectionName = 'data'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('TO', pymongo.DESCENDING)])

  return None


def createCandleMonthIndex(mongoHandle):
  '''
  Create index into mongoDB 'CandleMonth.data'
  '''
  tableName = 'CandleMonth'
  collectionName = 'data'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('TO', pymongo.DESCENDING)])

  return None


def createDealIndex(mongoHandle):
  '''
  Create index into mongoDB 'DealData.data'
  '''
  tableName = 'DealData'
  collectionName = 'data'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('_id.TO', pymongo.DESCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING), ('_id.TO', pymongo.DESCENDING)])

  return None


def createInfoIndex(mongoHandle):
  '''
  Create index into mongoDB 'INFO.INFO'
  '''
  tableName = 'INFO'
  collectionName = 'INFO'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('UT', pymongo.DESCENDING)])

  return None


def createRightIndex(mongoHandle):
  '''
  Create index into mongoDB 'INFO.RIGHT'
  '''
  tableName = 'INFO'
  collectionName = 'RIGHT'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])
  mongoHandle.createIndex(tableName, collectionName, [('_id.TO', pymongo.DESCENDING)])

  return None


def createQuoteIndex(mongoHandle):
  '''
  Create index into mongoDB 'QuoteData.data'
  '''
  tableName = 'QuoteData'
  collectionName = 'data'
  mongoHandle.createIndex(tableName, collectionName, [('_id.IC', pymongo.ASCENDING)])

  return None


def createIndex(mongoList):
  '''
  创建索引操作
  '''
  print 'createIndex >>'
  print mongoList
  logHandle = logFile.LogFile(name = 'CreateIndex')
  for mongo in mongoList:
    mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
    try:
      createCandleDayIndex(mongoHandle)
      logHandle.logInfo('Create [CandleDay] index succeed.')
      createCandleWeekIndex(mongoHandle)
      logHandle.logInfo('Create [CandleWeek] index succeed.')
      createCandleMonthIndex(mongoHandle)
      logHandle.logInfo('Create [CandleMonth] index succeed.')
      createDealIndex(mongoHandle)
      logHandle.logInfo('Create [DealData] index succeed.')
      createInfoIndex(mongoHandle)
      logHandle.logInfo('Create [StockInfo] index succeed.')
      createRightIndex(mongoHandle)
      logHandle.logInfo('Create [DivRightInfo] index succeed.')
      createQuoteIndex(mongoHandle)
      logHandle.logInfo('Create [QuoteData] index succeed.')
    except:
      logHandle.logInfo(str(traceback.format_exc()))
      print traceback.format_exc()
  print 'createIndex <<'
  return None


def startCreateIndex(mongoList, atOnce = False):
  '''
  创建启动进程
  '''
  if atOnce:
    createIndex(mongoList)

  while True:
    try:
      #Get date time now
      startTime = datetime.datetime.now()
      timetuple = startTime.timetuple()
      executeTime = datetime.datetime(
        year = timetuple.tm_year,
        month = timetuple.tm_mon,
        day = timetuple.tm_mday,
        hour = setup.setupDict['CreateIndex']['Schedule']['Hour'],
        minute = setup.setupDict['CreateIndex']['Schedule']['Minute'],
        second = setup.setupDict['CreateIndex']['Schedule']['Second'])
      #execute function next day this time
      if (executeTime < startTime):
        executeTime += datetime.timedelta(days=1)
        print 'CreateIndex Next execute time: %s' % executeTime.strftime('%Y-%m-%d %X')
      #Get delta time within next execute time and date time now.
      deltaTime = executeTime - startTime
      scheduleHandle = sched.scheduler(time.time, time.sleep)
      scheduleHandle.enter(deltaTime.total_seconds(), 1, createIndex, (mongoList, ))
      scheduleHandle.run()
    except:
      print traceback.format_exc()

  return None

if __name__ == '__main__':
  mongoList = [dbServer.mongoDbServer['22']['LAN'], dbServer.mongoDbServer['23']['LAN']]
  startCreateIndex(mongoList, atOnce=True)

