# -*- coding: utf-8 -*-
"""
Created on Fri Sep 23 10:50:23 2022

@author: MC
"""


import numpy as np
import matplotlib.pyplot as plt
import pymysql
import pandas as pd
import func1
import time
import datetime 
import math
import os
from PIL import Image
import copy
from alive_progress import alive_bar
import HStopLoss


def GetTradingDate():
    # 399905 中证500的交易日
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    TradingDate = []
    try:
        sql =  "select * from d_399905 " #%(stock,DDate)
        readsql = pd.read_sql(sql,connect)
        TradingDate = readsql['Time']
        TradingDate = TradingDate.apply(lambda x:str(x)[0:10])
        TradingDate = np.array(TradingDate)
        TradingDate.sort()
    except:
        print('Get Trading Date Failed')
    cur.close()
    connect.close()
    return TradingDate

def DownloadData(stock):
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    StockData = []
    try:
        sql = 'select * from m1_%s ORDER BY Time'%stock
        readsql = pd.read_sql(sql,connect)
        StockData = readsql
    except:
        print('Donwload Stock Data Failed')
    return StockData
    
if __name__ == '__main__' :
    
    slist = np.array(['600309'])
    #stock_list = slist #rd_j.apply(lambda x:'m1_'+(x[2:8]))
    #stock_list2 = slist.apply(lambda x:'d_'+(x[3:9]))
    
    TradingDate = GetTradingDate()
    StartDate = '2022-01-01'
    EndDate = '2022-09-20'
    TestingDate = TradingDate[TradingDate>StartDate]
    TestingDate = TestingDate[TestingDate<EndDate]
    for stock in slist:
        i = 0
        StockData = DownloadData(stock)
        sDate = StockData['Time']
        sDate = sDate.apply(lambda x:str(x)[0:10])
        #StockData['Time'] = sDate
        #sDate = np.array(sDate)
        StockData['CreateTime'] = sDate
        sDate = sDate.unique()
        TestingDate = np.intersect1d(sDate,TestingDate)
        for DDate in TestingDate:
            i += 1
            if(i<5):
                continue
            preDate = TestingDate[i-2]
            mData = StockData[StockData['CreateTime']==DDate]
            
            
            
    
    
            
            
            
            












