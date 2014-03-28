# -*- coding: UTF-8 -*-
'''
Created on 2013-11-19

@author: Grayson
'''


class Heap(object):
  '''
  千牛堆类型，插入记录后返回最大值与最小值
  '''
  def __init__(self, lens):
    '''
    Constructor, set max list length.
    '''
    self.list = list()
    self.lens = lens

    return None


  def __del__(self):
    if (self.list is not None):
      del self.list


  def append(self, close):
    '''
    向堆中插入记录
    '''
    self.list.append(close)
    while (len(self.list) > self.lens):
      self.list.pop(0)

    return None
      
      
  def object(self):
    '''
    Return object list.
    '''
    return self.list
  
  
  def max(self):
    '''
    Return max record.
    '''
    return max(self.list)
  
  
  def min(self):
    '''
    Return min record.
    '''
    return min(self.list)
