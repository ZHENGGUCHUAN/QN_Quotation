# -*- coding: UTF-8 -*-
'''
Created on 2014-03-28

@author: Grayson
'''
import sys, os, traceback, collections
sys.path.append(os.getcwd() + r'\Candle')
import QN_constant
sys.path.append(os.getcwd() + r'\..\Common')
import mongoAPI, logFile, common


class UpdateMongo(object):
  '''
  历史行情数据写入Mongo
  '''
  def __init__(self, mongoList):
    # Mongo句柄
    self.mongoHandleList = list()
    for mongo in mongoList:
      mongoHandle = mongoAPI.MongoAPI(mongo['SERVER'], mongo['PORT'])
      self.mongoHandleList.append(mongoHandle)
    # LogFile句柄
    self.logHandle = logFile.LogFile(name = 'Candle')


  def __del__(self):
    for mongoHandle in self.mongoHandleList:
      if (mongoHandle is not None):
        del mongoHandle
    del self.mongoHandleList
    if (self.logHandle is not None):
      del self.logHandle


  def updateMongo(self, updateData):
    try:
      infoDict = collections.OrderedDict()
      idDict = collections.OrderedDict()
      if (updateData['DB'] == 'DAY'):
        timeOffset = common.datetime2days(updateData['ANA']['TRADE_DATE'])
        idDict['IC'] = int(updateData['ANA']['INNER_CODE'])
        idDict['TO'] = timeOffset
        infoDict['_id'] = idDict
        infoDict['TO'] = timeOffset
      elif (updateData['DB'] == 'WEEK'):
        idDict['IC'] = int(updateData['ANA']['INNER_CODE'])
        idDict['YR'] = updateData['ANA']['TRADE_YEAR']
        idDict['WK'] = updateData['ANA']['TRADE_WEEK']
        infoDict['_id'] = idDict
        infoDict['TO'] = common.datetime2days(updateData['ANA']['LAST_TRADE_DATE'])
      elif (updateData['DB'] == 'MONTH'):
        idDict['IC'] = int(updateData['ANA']['INNER_CODE'])
        idDict['YR'] = updateData['ANA']['TRADE_YEAR']
        idDict['MN'] = updateData['ANA']['TRADE_MONTH']
        infoDict['_id'] = idDict
        infoDict['TO'] = common.datetime2days(updateData['ANA']['LAST_TRADE_DATE'])
      infoDict['OP'] = float(updateData['ANA']['TOPEN'])
      infoDict['CP'] = float(updateData['ANA']['TCLOSE'])
      infoDict['HP'] = float(updateData['ANA']['THIGH'])
      infoDict['LP'] = float(updateData['ANA']['TLOW'])
      infoDict['VL'] = long(updateData['ANA']['TVOLUME'])
      infoDict['AM'] = float(updateData['ANA']['TVALUE'])
      infoDict['TR'] = float(updateData['ANA']['EXCHR']) if (updateData['ANA']['EXCHR'] is not None) else float(0)
      infoDict['CA'] = float(updateData['ANA']['CHNG']) if (updateData['ANA']['CHNG'] is not None) else float(0)
      infoDict['AA'] = [float(updateData['EXT']['MA5']), float(updateData['EXT']['MA10']), float(updateData['EXT']['MA13']),
                        float(updateData['EXT']['MA14']), float(updateData['EXT']['MA20']),float(updateData['EXT']['MA25']),
                        float(updateData['EXT']['MA43'])]
      infoDict['AV'] = [long(updateData['EXT']['VOL5']), long(updateData['EXT']['VOL10']), long(updateData['EXT']['VOL30']),
                        long(updateData['EXT']['VOL60']), long(updateData['EXT']['VOL135'])]
      infoDict['KV'] = [float(updateData['EXT']['K9']), float(updateData['EXT']['K34'])]
      infoDict['DV'] = [float(updateData['EXT']['D3']), float(updateData['EXT']['D9'])]
      infoDict['JV'] = [float(updateData['EXT']['J3']), float(updateData['EXT']['J9'])]
      infoDict['DIF'] = [float(updateData['EXT']['DIF1']), float(updateData['EXT']['DIF2']), float(updateData['EXT']['DIF3'])]
      infoDict['DEA'] = [float(updateData['EXT']['DEA1']), float(updateData['EXT']['DEA2']), float(updateData['EXT']['DEA3'])]
      infoDict['MACD'] = [float(updateData['EXT']['MACD1']), float(updateData['EXT']['MACD2']), float(updateData['EXT']['MACD3'])]
      infoDict['ER'] = int(updateData['XRXD'])
      infoDict['RSV'] = [float(updateData['EXT']['RSV9']), float(updateData['EXT']['RSV34'])]
      infoDict['EMA'] = [float(updateData['EXT']['EMA5']), float(updateData['EXT']['EMA6']), float(updateData['EXT']['EMA12']),
                         float(updateData['EXT']['EMA13']), float(updateData['EXT']['EMA26']), float(updateData['EXT']['EMA35'])]

      for mongoHandle in self.mongoHandleList:
        mongoHandle.update(QN_constant.dbDict[updateData['DB']]['TABLE']['MONGO'], 'data', {'_id':idDict}, infoDict)
        #并表数据写入
        if (updateData['WMT']):
          mongoHandle.update(QN_constant.dbDict[updateData['DB']]['TABLE']['MERGE'], 'data', {'_id':idDict}, infoDict)
    except:
      self.logHandle.logInfo(str(traceback.format_exc()))
      print traceback.format_exc()

    return None
