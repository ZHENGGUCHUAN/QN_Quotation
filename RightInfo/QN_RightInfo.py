# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback, datetime, sched, time, collections, pymongo
import setup
sys.path.append(os.getcwd() + r'\..\Common')
import mssqlAPI, mongoAPI, logFile, common


class RightInfo(object):
  def __init__(self, mssqlDict, mongoList):
    # MSSQL句柄
    self.mssqlHandle = mssqlAPI.MssqlAPI(server = mssqlDict['SERVER'], db = mssqlDict['DB'], user = mssqlDict['USER'], pwd = mssqlDict['PWD'])
    # Mongo句柄
    self.mongoHandleList = list()
    for mongo in mongoList:
      mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
      self.mongoHandleList.append(mongoHandle)
    # LogFile句柄
    self.logHandle = logFile.LogFile(name = 'RightInfo')

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
    Query stock info from MSSQL.
    '''
    queryStr = '''
      SELECT
        INNER_CODE,
        EX_DIVI_DATE,
        CASH_BT,
        CAP_SHR,
        ALLOT_PCT,
        BONUS_SHR,
        ALLOT_PRC
      FROM
        [QN_Quotation].[dbo].[QN_DIV_INFO]
    '''
    if self.mssqlHandle is not None:
      return self.mssqlHandle.sqlQuery(queryStr)
    else:
      print 'mssqlHandle is None.'
      return None


  def getRecentTradeTime(self):
    '''
    从中间库获取所有股票最后更新记录时间
    '''
    try:
      sqlCmd = '''
        SELECT
          [INNER_CODE],
          [TRADE_DATE]
        FROM(
          SELECT
            [INNER_CODE],
            [TRADE_DATE],
            ROW_NUMBER() OVER(PARTITION BY INNER_CODE ORDER BY TRADE_DATE DESC) AS DESCENT
          FROM
            [QN_Quotation].[dbo].[SECURITY_ANALYSIS_DAY]
          ) t
        WHERE
          DESCENT = '1'
      '''
      recentDict = dict()
      records = self.mssqlHandle.sqlQuery(sqlCmd)
      for record in records:
        recentDict[str(record['INNER_CODE'])] = common.datetime2days(record['TRADE_DATE'])

      return recentDict
    except IndexError:
      self.logHandle.logInfo(str(traceback.format_exc()))
      print 'Get stock lately update time failed.'

      return None


  def setData(self, records):
    '''
    Insert stock info into mongo.
    '''
    try:
      for mongoHandle in self.mongoHandle:
        mongoHandle.remove()

      recordNum = 0
      recentDict = self.getRecentTradeTime()
      for record in records:
        #Get inward code
        inwardId = record['INNER_CODE']
        keyDoc = collections.OrderedDict()
        keyDoc['IC'] = int(inwardId)
        keyDoc['TO'] = int(common.datetime2days(record['EX_DIVI_DATE']))
        #spec
        spec = collections.OrderedDict()
        spec['_id'] = keyDoc
        #document
        document = collections.OrderedDict()
        document['_id'] = keyDoc
        document['BS'] = float(0) if (record['CASH_BT'] is None) else float(record['CASH_BT'])
        document['PR'] = float(0) if (record['ALLOT_PRC'] is None) else float(record['ALLOT_PRC'])
        CirculationStockChangeRatio = float(0)
        CirculationStockChangeRatio += float(0) if (record['CAP_SHR'] is None) else float(record['CAP_SHR'])
        CirculationStockChangeRatio += float(0) if (record['ALLOT_PCT'] is None) else float(record['ALLOT_PCT'])
        CirculationStockChangeRatio += float(0) if (record['BONUS_SHR'] is None) else float(record['BONUS_SHR'])
        document['FR'] = CirculationStockChangeRatio / float(10)

        for mongoHandle in self.mongoHandle:
          if (recentDict.get(inwardId) is None):
            document['EF'] = int(0)
          elif (self.recentDict.get(inwardId) >= keyDoc['TO']):
            document['EF'] = int(1)
          else:
            document['EF'] = int(0)
          mongoHandle.update('INFO', 'RIGHT', spec, document)
        recordNum += 1
      self.logFileInstance.logInfo('Update ration succeed, update records: ' + str(recordNum))
    except Exception,e:
      self.logFileInstance.logInfo('Update ration failed, update records: ' + str(recordNum) + '/' + str(len(records)) + ', exception: ' + str(e))
    #insert index
    index = [[('_id.IC', pymongo.ASCENDING), ('_id.TO', pymongo.ASCENDING)], [('_id.TO', pymongo .ASCENDING)], [('EF', pymongo.ASCENDING)]]
    for mongoHandle in self.disposeDbRationInstance.mongoHandle:
      mongoHandle.createIndex('INFO', 'RIGHT', index)

    return None


def rightInfo(mssqlDict, mongoList):
  '''
  更新股票信息操作
  '''
  print 'rightInfo >>'
  rightInfoHandle = RightInfo(mssqlDict = mssqlDict, mongoList = mongoList)
  records = rightInfoHandle.getData()
  if (records is not None):
    rightInfoHandle.setData(records)
  print 'rightInfo <<'
  return None


def startRightInfo(mssqlDict, mongoList, atOnce = False):
  if atOnce:
    rightInfo(mssqlDict, mongoList)

  while True:
    try:
      #Get date time now
      startTime = datetime.datetime.now()
      timetuple = startTime.timetuple()
      executeTime = datetime.datetime(
        year = timetuple.tm_year,
        month = timetuple.tm_mon,
        day = timetuple.tm_mday,
        hour = setup.setupDict['RightInfo']['Schedule']['Hour'],
        minute = setup.setupDict['RightInfo']['Schedule']['Minute'],
        second = setup.setupDict['RightInfo']['Schedule']['Second'])
      #execute function next day this time
      if (executeTime < startTime):
        executeTime += datetime.timedelta(days=1)
        print 'RightInfo next execute time: %s' % executeTime.strftime('%Y-%m-%d %X')
      #Get delta time within next execute time and date time now.
      deltaTime = executeTime - startTime
      scheduleHandle = sched.scheduler(time.time, time.sleep)
      scheduleHandle.enter(deltaTime.total_seconds(), 1, rightInfo, (mssqlDict, mongoList))
      scheduleHandle.run()
    except:
      print traceback.format_exc()
