# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback, datetime, sched, time, collections
import setup
sys.path.append(os.getcwd() + r'\..\Common')
import mssqlAPI, mongoAPI, logFile, dbServer


class StockInfo(object):
  def __init__(self, mssqlDict, mongoList):
    # MSSQL句柄
    self.mssqlHandle = mssqlAPI.MssqlAPI(server = mssqlDict['SERVER'], db = mssqlDict['DB'], user = mssqlDict['USER'], pwd = mssqlDict['PWD'])
    # Mongo句柄
    self.mongoHandleList = list()
    for mongo in mongoList:
      mongoHandle = mongoAPI.MongoAPI(server = mongo['SERVER'], port = mongo['PORT'])
      self.mongoHandleList.append(mongoHandle)
    # LogFile句柄
    self.logHandle = logFile.LogFile(name = 'StockInfo')

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
        INNER_CODE AS DM,
        SEC_SNAME AS MC,
        SEC_CHI_SPEL AS JC,
        CASE
          WHEN MKT_TYPE = '1' THEN 'SZ'
          WHEN MKT_TYPE = '2' THEN 'SH'
          ELSE 'INVALID'
        END AS JYS,
        GETDATE() AS MT,
        GETDATE() AS RT,
        LIST_ENDDATE AS ZZSJ
      FROM [QN_Quotation].[dbo].[SECURITY_INFO]

      UNION all

      SELECT
        '231' + RIGHT('00000' + CAST(areaId AS VARCHAR(10)), 5) AS DM,
        name AS MC,
        '' AS JC,
        'QN' AS JYS,
        GETDATE() AS MT,
        GETDATE() AS RT,
        NULL AS ZZSJ
      FROM conf_area

      UNION all

      SELECT
        '233' + RIGHT('00000' + CAST(industryId AS VARCHAR(10)), 5) AS DM,
        LEFT(name,8) AS MC,
        '' AS JC,
        'QN' AS JYS,
        GETDATE() AS MT,
        GETDATE() AS RT,
        NULL AS ZZSJ
      FROM conf_industry
      '''
    if self.mssqlHandle is not None:
      return self.mssqlHandle.sqlQuery(queryStr)
    else:
      print 'mssqlHandle is None.'
      return None


  def setData(self, records):
    '''
    Insert stock info into mongo.
    '''
    try:
      recordNum = 0
      for record in records:
        document = collections.OrderedDict()
        document['_id'] = {'IC': int(record['DM'])}
        modifiedTime = datetime.datetime(2011,9,27) if (record['MT'] is None) else record['MT']
        recordTime = datetime.datetime(2011,9,27) if (record['RT'] is None) else record['RT']
        document['UT'] = modifiedTime if (modifiedTime >= recordTime) else recordTime
        document['EC'] = str(record['DM'])[-6:].strip()
        document['SM'] = record['JYS'].strip()
        document['AN'] = '' if (record['JC'] is None) else record['JC'].strip()
        document['SN'] = record['MC'].strip()
        if ((record['ZZSJ'] is not None) and (record['ZZSJ'] != 0)):
          document['DT'] = record['ZZSJ']
        for mongoHandle in self.mongoHandleList:
          #更新已存在的股票信息
          spec = {'_id.IC' : int(record['DM']), 'SN' : {'$ne': record['MC'].strip()}}
          mongoHandle.update('INFO', 'INFO', spec, document, upsert = False)
          #添加不存在的股票信息
          mongoHandle.insert('INFO', 'INFO', document, catchException = False)
        recordNum += 1
      self.logHandle.logInfo('Update security info succeed, update records: ' + str(recordNum))
    except:
      self.logHandle.logInfo(
        'Update security info failed, update records: ' + str(recordNum) + '/' + str(len(records)) +
        ', exception: ' + str(traceback.format_exc()))
      print traceback.format_exc()

    return None


def stockInfo(mssqlDict, mongoList):
  '''
  更新股票信息操作
  '''
  print 'stockInfo >>'
  stockInfoHandle = StockInfo(mssqlDict = mssqlDict, mongoList = mongoList)
  records = stockInfoHandle.getData()
  if (records is not None):
    stockInfoHandle.setData(records)
  print 'stockInfo <<'
  return None


def startStockInfo(mssqlDict, mongoList, atOnce = False):
  if atOnce:
    stockInfo(mssqlDict, mongoList)

  while True:
    try:
      #Get date time now
      startTime = datetime.datetime.now()
      timetuple = startTime.timetuple()
      executeTime = datetime.datetime(
        year = timetuple.tm_year,
        month = timetuple.tm_mon,
        day = timetuple.tm_mday,
        hour = setup.setupDict['StockInfo']['Schedule']['Hour'],
        minute = setup.setupDict['StockInfo']['Schedule']['Minute'],
        second = setup.setupDict['StockInfo']['Schedule']['Second'])
      #execute function next day this time
      if (executeTime < startTime):
        executeTime += datetime.timedelta(days=1)
        print 'StockInfo next execute time: %s' % executeTime.strftime('%Y-%m-%d %X')
      #Get delta time within next execute time and date time now.
      deltaTime = executeTime - startTime
      scheduleHandle = sched.scheduler(time.time, time.sleep)
      scheduleHandle.enter(deltaTime.total_seconds(), 1, stockInfo, (mssqlDict, mongoList))
      scheduleHandle.run()
    except:
      print traceback.format_exc()


if __name__ == '__main__':
  mssqlDict = dbServer.mssqlDbServer['46']['LAN']
  mongoList = [dbServer.mongoDbServer['22']['LAN'], dbServer.mongoDbServer['23']['LAN']]
  startStockInfo(mssqlDict, mongoList, atOnce=True)
