# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''
import sys, os, traceback
sys.path.append(os.getcwd() + r'\Candle')
import QN_constant
sys.path.append(os.getcwd() + r'\..\Common')
import mssqlAPI, logFile


class QueryMssql(object):
  '''
  从SQL中间库获取历史行情数据
  '''
  def __init__(self, mssqlDict, queryHandle):
    self.mssqlHandle = mssqlAPI.MssqlAPI(
      server = mssqlDict['SERVER'],
      db = mssqlDict['DB'],
      user = mssqlDict['USER'],
      pwd = mssqlDict['PWD'])
    self.logFileHandle = logFile.LogFile(name='Candle')
    self.queryHandle = queryHandle

    return None


  def __del__(self):
    if (self.mssqlHandle is not None):
      del self.mssqlHandle
    if (self.logFileHandle is not None):
      del self.logFileHandle

    return None


  def getStockCodeList(self):
    stockCodeList = list()
    sqlCmd = '''
      SELECT
        INNER_CODE
      FROM
      ''' + QN_constant.dbDict['DAY']['TABLE']['ANA'] + '''
      GROUP BY
        INNER_CODE
      '''
    records = self.mssqlHandle.sqlQuery(sqlCmd)
    if (records is not None):
      for record in records:
        stockCodeList.append(str(record['INNER_CODE']))

    return stockCodeList


  def getSpecifyData(self, db, codeList):
    '''
    获取指定代码历史行情数据
    '''
    condition = 'INNER_CODE IN ('
    for code in codeList:
      condition += "'" + str(code) + "',"
    condition = condition[0:-1]
    condition += ')'
    if ((db == 'WEEK') and (db == 'MONTH')):
      condition += ' AND [SECURITY_ANALYSIS].[TRADE_DAYS] > 0'
    self.__getdata(db, condition)

    return None


  def getData(self, db):
    '''
    获取所有股票历史行情数据
    '''
    if ((db == 'WEEK') and (db == 'MONTH')):
      condition = '[SECURITY_ANALYSIS].[TRADE_DAYS] > 0'
    else:
      condition = '1 = 1'
    self.__getdata(db, condition)


  def __getdata(self, db, condition):
    '''
    private method
    #获取指定证券代码列表所示分析信息，以单个证券代码为单位由queue传递出去
    #传出消息按如下字典格式:
    {'DB'  :'DAY/WEEK/MONTH',
     'MSG' :[{'INNER_CODE':<INNER_CODE>,
              'LCLOSE':<LCLOSE1>,
              'TOPEN':<TOPEN1>,
              'TCLOSE':<TCLOSE1>,
              'THIGH':<THIGH1>,
              'TLOW':<TLOW1>,
              'TVOLUME':<TVOLUME1>,
              'TVALUE':<TVALUE1>,
              ... ...
             },
             {'INNER_CODE':<INNER_CODE>,
              'LCLOSE':<LCLOSE2>,
              'TOPEN':<TOPEN2>,
              'TCLOSE':<TCLOSE2>,
              'THIGH':<THIGH2>,
              'TLOW':<TLOW2>,
              'TVOLUME':<TVOLUME2>,
              'TVALUE':<TVALUE2>,
              ... ...},
             ... ...]
    }
    '''
    try:
      if ((db == 'WEEK') or (db == 'MONTH')):
        field = '''
          [SECURITY_ANALYSIS].[FIRST_TRADE_DATE] AS FDATE,
          [SECURITY_ANALYSIS].[LAST_TRADE_DATE] AS LDATE,
        '''
      else:
        field = '''
          [SECURITY_ANALYSIS].[TRADE_DATE] AS FDATE,
          [SECURITY_ANALYSIS].[TRADE_DATE] AS LDATE,
        '''
      #Instruct SQL command
      sqlCmd = '''
        SELECT
          [SECURITY_ANALYSIS].[INNER_CODE] AS INNER_CODE,
          [SECURITY_ANALYSIS].[LCLOSE] AS LCLOSE,
          [SECURITY_ANALYSIS].[TOPEN] AS TOPEN,
          [SECURITY_ANALYSIS].[TCLOSE] AS TCLOSE,
          [SECURITY_ANALYSIS].[THIGH] AS THIGH,
          [SECURITY_ANALYSIS].[TLOW] AS TLOW,
          [SECURITY_ANALYSIS].[TVOLUME] AS TVOLUME,
          [SECURITY_ANALYSIS].[TVALUE] AS TVALUE,
          [SECURITY_ANALYSIS].[CHNG] AS CHNG,
          [SECURITY_ANALYSIS].[EXCHR] AS EXCHR,
          ''' + field + '''
          ''' + QN_constant.dbDict[db]['TRADEDATE'] + '''
        FROM
          ''' + QN_constant.dbDict[db]['TABLE']['ANA'] + ''' AS [SECURITY_ANALYSIS]
        WHERE
          ''' + condition + '''
        ORDER BY
          INNER_CODE,
          ''' + QN_constant.dbDict[db]['ORDER']
      records = self.mssqlHandle.sqlQuery(sqlCmd)

      innerCode = ''
      #单个证券多日衍生数据列表
      singleStockDataList = list()
      #遍历查询记录
      for rec in records:
        infoDict = dict()
        #遍历基础信息字段
        for column in QN_constant.dbDict[db]['COLUMN']['ANA']:
          #查询结果以字段名称为key值存入有序字典
          infoDict[column] = rec[column]
        #if ((db == 'WEEK') or (db == 'MONTH')):
        infoDict['FDATE'] = rec['FDATE']
        infoDict['LDATE'] = rec['LDATE']
        #不同股票写入不同字典键值中
        if ((rec['INNER_CODE'] != innerCode) and (innerCode != '')):
          self.queryHandle.put({'DB':db,'MSG':singleStockDataList})
          singleStockDataList = list()
          innerCode = rec['INNER_CODE']
        elif (innerCode == ''):
          innerCode = rec['INNER_CODE']
        #将有序字典追加到结果集列表中
        singleStockDataList.append(infoDict)
      else:
        self.queryHandle.put({'DB':db,'MSG':singleStockDataList})
    except IndexError:
      self.logFileHandle.logInfo(str(traceback.format_exc()))
      print 'Invalid table name.'

    return None
