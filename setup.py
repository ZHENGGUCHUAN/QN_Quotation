# -*- coding: UTF-8 -*-
'''
Created on 2014-03-27

@author: Grayson
'''

setupDict = {
  # 历史K线信息
  'Candle'      :{'Valid':True, 'AtOnce':False, 'Schedule':{'Hour':1, 'Minute':0, 'Second':0, 'DayInterval':1}},
  # 证券基本信息
  'StockInfo'   :{'Valid':True, 'AtOnce':False, 'Schedule':{'Hour':6, 'Minute':0, 'Second':0, 'DayInterval':1}},
  # 证券权息信息
  'RightInfo'   :{'Valid':True, 'AtOnce':False, 'Schedule':{'Hour':6, 'Minute':0, 'Second':0, 'DayInterval':1}},
  # 复权收盘价信息
  'RestoreClose':{'Valid':True, 'AtOnce':False, 'Schedule':{'Hour':6, 'Minute':0, 'Second':0, 'DayInterval':1}},
  # 创建Mongo索引
  'CreateIndex' :{'Valid':True, 'AtOnce':False, 'Schedule':{'Hour':9, 'Minute':0, 'Second':0, 'DayInterval':1}},
  # 队列容量
  'QueueVolume' :{'QueryMssql':10, 'UpdateMssql':10000, 'UpdateMongo':10000},
  # 进程数量
  'ProcessNum'  :{'CalculateData':8, 'UpdateMssql':4, 'UpdateMongo':8}
}
