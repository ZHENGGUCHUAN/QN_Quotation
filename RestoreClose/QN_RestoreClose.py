# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback, collections, datetime, sched, time
import setup
sys.path.append(os.getcwd() + r'\..\Common')
import mssqlAPI, mongoAPI, logFile, dbServer


class RestoreClose(object):
  def __init__(self, mssqlDict, mongoList):
    # MSSQL句柄
    self.mssqlHandle = mssqlAPI.MssqlAPI(server = mssqlDict['SERVER'], db = mssqlDict['DB'], user = mssqlDict['USER'], pwd = mssqlDict['PWD'])
    # Mongo句柄
    self.mongoHandleList = list()
    for mongo in mongoList:
      mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
      self.mongoHandleList.append(mongoHandle)
    # LogFile句柄
    self.logHandle = logFile.LogFile(name = 'RestoreClose')

    return None


  def __del__(self):
    # MSSQL句柄
    if self.mssqlHandle is not None:
      del self.mssqlHandle
    # Mongo句柄
    for mongo in self.mongoHandleList:
      if mongo is not None:
        del mongo
    # LogFile句柄
    if self.logHandle is not None:
      del self.logHandle

    return None


  def getData(self):
    '''
    Query restore close price from MSSQL.
    '''
    try:
      sqlCmd = '''
        SELECT
          INNER_CODE,
          D5CLOSE AS [D5C],
          D10CLOSE AS [D10C],
          NYCLOSE AS [NYC],
          W52CLOSE AS [W52C]
        FROM
          QN_MULTIPLE_CLOSE
        '''
      return self.mssqlHandle.sqlQuery(sqlCmd)
    except:
      self.logHandle.logInfo(str(traceback.format_exc()))
      print traceback.format_exc()

    return None


  def setData(self, records):
    '''
    Insert restore close price into mongo.
    '''
    fieldList = ['D5C', 'D10C', 'NYC', 'W52C']
    try:
      recordNum = 0
      for record in records:
        spec = {'_id.IC': int(record['INNER_CODE'])}
        document = collections.OrderedDict()
        #证券内部编码
        document['_id'] = {'IC': int(record['INNER_CODE'])}
        for field in fieldList:
          if (record[field] is not None):
            document[field] = float(record[field])
          else:
            document[field] = float(0)
        #MongoDB写入
        for mongoHandle in self.mongoHandleList:
          mongoHandle.update('TEMP', 'MCLOSE', spec, document)
        recordNum += 1
      self.logHandle.logInfo('Sync close price succeed, sync records: ' + str(recordNum))
    except:
      self.logHandle.logInfo('Sync close price failed, sync records: ' + str(recordNum) + '/' + str(len(records)) + ', exception: ' + str(traceback.format_exc()))
      print traceback.format_exc()

    return None


def restoreClose(mssqlDict, mongoList):
  '''
  复权收盘价操作
  '''
  print 'restoreClose >>'
  restoreCloseHandle = RestoreClose(mssqlDict = mssqlDict, mongoList = mongoList)
  records = restoreCloseHandle.getData()
  if (records is not None):
    restoreCloseHandle.setData(records)
  print 'restoreClose <<'
  return None


def startRestoreClose(mssqlDict, mongoList, atOnce = False):
  '''
  复权收盘价启动进程
  '''
  if atOnce:
    restoreClose(mssqlDict, mongoList)

  while True:
    try:
      #Get date time now
      startTime = datetime.datetime.now()
      timetuple = startTime.timetuple()
      executeTime = datetime.datetime(
        year = timetuple.tm_year,
        month = timetuple.tm_mon,
        day = timetuple.tm_mday,
        hour = setup.setupDict['RestoreClose']['Schedule']['Hour'],
        minute = setup.setupDict['RestoreClose']['Schedule']['Minute'],
        second = setup.setupDict['RestoreClose']['Schedule']['Second'])
      #execute function next day this time
      if (executeTime < startTime):
        executeTime += datetime.timedelta(days=1)
        print 'RestoreClose next execute time: %s' % executeTime.strftime('%Y-%m-%d %X')
      #Get delta time within next execute time and date time now.
      deltaTime = executeTime - startTime
      scheduleHandle = sched.scheduler(time.time, time.sleep)
      scheduleHandle.enter(deltaTime.total_seconds(), 1, restoreClose, (mssqlDict, mongoList))
      scheduleHandle.run()
    except:
      print traceback.format_exc()

  return None


if __name__ == '__main__':
  mssqlDict = dbServer.mssqlDbServer['46']['LAN']
  mongoList = [dbServer.mongoDbServer['22']['LAN'], dbServer.mongoDbServer['23']['LAN']]
  startRestoreClose(mssqlDict, mongoList, atOnce=True)
