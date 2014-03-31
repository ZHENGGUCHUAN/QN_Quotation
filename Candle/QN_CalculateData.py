# -*- coding: UTF-8 -*-
'''
Created on 2014-03-28

@author: Grayson
'''
import sys, os, collections, datetime, decimal, traceback
sys.path.append(os.getcwd() + r'\Candle')
import QN_Heap, QN_constant
sys.path.append(os.getcwd() + r'\..\Common')
import logFile, common


class CalculateData(object):
  '''
  从历史基本数据计算出历史各指标衍生数据集
  '''
  def __init__(self, queueHandle):
    # LogFile句柄
    self.logHandle = logFile.LogFile(name = 'Candle')
    self.queueHandle = queueHandle


  def __del__(self):
    if (self.logHandle is not None):
      del self.logHandle


  def calculateData(self, db, singleStockDataList):
    '''
    计算个单股票各指标衍生数据集
    '''
    try:
      closeTuple = (9, 34)
      highDict = collections.OrderedDict()
      lowDict = collections.OrderedDict()
      for close in closeTuple:
        highDict[close] = QN_Heap.Heap(close)
        lowDict[close] = QN_Heap.Heap(close)
      #收盘价列表
      priceTuple = (5, 10, 13, 14, 20, 25, 43)
      priceDict = collections.OrderedDict()
      for price in priceTuple:
        priceDict[price] = QN_Heap.Heap(price)
      #成交金额列表
      volumeTuple = (5, 10, 30, 60, 135)
      volumeDict = collections.OrderedDict()
      for volume in volumeTuple:
        volumeDict[volume] = QN_Heap.Heap(volume)
      '''
      #除权除息日
      noneExcludeInfo = {'date':datetime.datetime(1970,1,1), 'type':3}
      if (len(excludeRightDividendList) > 0):
        excludeInfo = excludeRightDividendList[0]
      else:
        excludeInfo = noneExcludeInfo
      '''
      #现有记录数量
      recordNum = 0
      recentExtensionInfo = dict()
      #日周月合并表导入最多61条记录信息
      totalRecordNum = len(singleStockDataList)
      #遍历指定股票全部信息
      for stockData in singleStockDataList:
        if (totalRecordNum - recordNum <= 61):
          writeMergeTable = True
        else:
          writeMergeTable = False
        #衍生记录
        extensionInfo = collections.OrderedDict()
        extensionInfo['INNER_CODE'] = stockData['INNER_CODE']
        excludeType = 3
        if (db == 'DAY'):
          extensionInfo['TRADE_DATE'] = stockData['TRADE_DATE']
          '''
          #除权除息信息
          if ((stockData['TRADE_DATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
            excludeType = excludeInfo['type']
            try:
              excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
            except IndexError:
              excludeInfo = noneExcludeInfo
          else:
            excludeType = 3
          '''
        elif (db == 'WEEK'):
          extensionInfo['TRADE_YEAR'] = stockData['TRADE_YEAR']
          extensionInfo['TRADE_WEEK'] = stockData['TRADE_WEEK']
          '''
          #除权除息信息
          if ((stockData['FDATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
            excludeType = excludeInfo['type']
            try:
              excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
            except IndexError:
              excludeInfo = noneExcludeInfo
          else:
            excludeType = 3
          '''
        elif (db == 'MONTH'):
          extensionInfo['TRADE_YEAR'] = stockData['TRADE_YEAR']
          extensionInfo['TRADE_MONTH'] = stockData['TRADE_MONTH']
          '''
          #除权除息信息
          if ((stockData['FDATE'] >= excludeInfo['date']) and (excludeInfo['date'] != noneExcludeInfo['date'])):
            excludeType = excludeInfo['type']
            try:
              excludeInfo = excludeRightDividendList[excludeRightDividendList.index(excludeInfo)+1]
            except IndexError:
              excludeInfo = noneExcludeInfo
          else:
            excludeType = 3
          '''
        #收盘价列表
        for closeType in closeTuple:
          highDict[closeType].append(stockData['THIGH'])
          lowDict[closeType].append(stockData['TLOW'])
        #收盘价列表
        for priceType in priceTuple:
          priceDict[priceType].append(int(stockData['TCLOSE'] * 65536))
        #成交量列表
        for volumeType in volumeTuple:
          volumeDict[volumeType].append(int(stockData['TVOLUME'] * 65536))
        #成交价格均值
        for priceType in priceTuple:
          sumPrice = int(0)
          for price in priceDict[priceType].object():
            sumPrice += int(price)
          sumPrice = decimal.Decimal(sumPrice) / 65536
          extensionInfo['MA' + str(priceType)] = decimal.Decimal(sumPrice) / len(priceDict[priceType].object())
        #成交量均值
        for volumeType in volumeTuple:
          sumVolume = int(0)
          for volume in volumeDict[volumeType].object():
            sumVolume += int(volume)
          sumVolume = decimal.Decimal(sumVolume) / 65536
          extensionInfo['VOL' + str(volumeType)] = decimal.Decimal(sumVolume) / len(volumeDict[volumeType].object())
        #KDJ值
        for kdj in QN_constant.constantDict['KDJ']:
          if (recordNum < 1):
            if (common.equalZero(stockData['THIGH'] - stockData['TLOW']) == True):
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']] = QN_constant.constantDict['ZERO']
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']] = QN_constant.constantDict['ZERO']
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']] = QN_constant.constantDict['ZERO']
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['J']] = QN_constant.constantDict['ZERO']
            else:
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']] = (stockData['TCLOSE'] - stockData['TLOW']) / (stockData['THIGH'] - stockData['TLOW']) * 100
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']] = extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']]
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']] = extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']]
              extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['J']] = extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']]
          else:
            extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']] = \
              decimal.Decimal(0) if (common.equalZero(highDict[kdj[0]].max() - lowDict[kdj[0]].min()) == True) \
              else ((stockData['TCLOSE'] - lowDict[kdj[0]].min()) / (highDict[kdj[0]].max() - lowDict[kdj[0]].min()) * 100)
            extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']] = \
              QN_constant.constantDict['KDJ'][kdj]['PARA']['KP1'] * recentExtensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']] + \
              QN_constant.constantDict['KDJ'][kdj]['PARA']['KP2'] * extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['RSV']]
            extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']] = \
              QN_constant.constantDict['KDJ'][kdj]['PARA']['DP1'] * recentExtensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']] + \
              QN_constant.constantDict['KDJ'][kdj]['PARA']['DP2'] * extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']]
            extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['J']] = \
              3 * extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']] - \
              2 * extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']]

          recentExtensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']] = extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['K']]
          recentExtensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']] = extensionInfo[QN_constant.constantDict['KDJ'][kdj]['NAME']['D']]
        #EMA值
        for ema in QN_constant.constantDict['EMA']:
          if (recordNum >= 2):
            extensionInfo[QN_constant.constantDict['EMA'][ema]['NAME']] = \
              QN_constant.constantDict['EMA'][ema]['PARA']['P1'] * stockData['TCLOSE'] + \
              QN_constant.constantDict['EMA'][ema]['PARA']['P2'] * recentExtensionInfo[QN_constant.constantDict['EMA'][ema]['NAME']]
          elif (recordNum == 1):
            extensionInfo[QN_constant.constantDict['EMA'][ema]['NAME']] = \
              QN_constant.constantDict['EMA'][ema]['PARA']['P1'] * stockData['TCLOSE'] + \
              QN_constant.constantDict['EMA'][ema]['PARA']['P2'] * stockData['LCLOSE']
          else:
            extensionInfo[QN_constant.constantDict['EMA'][ema]['NAME']] = stockData['TCLOSE']

          recentExtensionInfo[QN_constant.constantDict['EMA'][ema]['NAME']] = extensionInfo[QN_constant.constantDict['EMA'][ema]['NAME']]
        #MACD值
        for macd in QN_constant.constantDict['MACD']:
          if (recordNum >= 1):
            extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DIF']] = \
              extensionInfo['EMA'+str(macd[0])] - extensionInfo['EMA'+str(macd[1])]
            extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DEA']] = \
              QN_constant.constantDict['MACD'][macd]['PARA']['P1'] * extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DIF']] + \
              QN_constant.constantDict['MACD'][macd]['PARA']['P2'] * recentExtensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DEA']]
            extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['MACD']] = \
              (extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DIF']] - extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DEA']]) * 2
          else:
            extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DIF']] = QN_constant.constantDict['ZERO']
            extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DEA']] = QN_constant.constantDict['ZERO']
            extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['MACD']] = QN_constant.constantDict['ZERO']

          recentExtensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DEA']] = extensionInfo[QN_constant.constantDict['MACD'][macd]['NAME']['DEA']]
        #记录数量
        recordNum += 1

        #基本数据及衍生数据写入队列
        self.queueHandle.put({'DB':db,'ANA':stockData,'EXT':extensionInfo, 'XRXD':excludeType, 'WMT':writeMergeTable})
    except:
      self.logHandle.logInfo(str(traceback.format_exc()))
      print traceback.format_exc()

    return None
