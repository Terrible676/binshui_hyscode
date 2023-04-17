# -*- coding: utf-8 -*-
"""
Created on Mon Mar 20 16:48:24 2023

@author: MC
"""

# Volume*(Volitility)^2*abs(sum(rtn,5))

import pandas as pd
import numpy as np
import copy
import matplotlib.pyplot as plt
import pymysql
import time
import datetime
import os
from alive_progress  import alive_bar
import math


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

def DownloadMinData(stock): # 读取分钟数据
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
    cur.close()
    connect.close()
    return StockData

def DownloadDayData(stock): #读取日k数据
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    #print(cur)
    StockData = []
    try:
        StartDate = datetime.datetime.strptime('2022-12-01','%Y-%m-%d')
        sql = "select * from d_%s WHERE Time > '%s' ORDER BY Time" %(stock,StartDate)
        readsql = pd.read_sql(sql,connect)
        StockData = readsql
    except:
        print('Donwload Stock Data Failed')
    cur.close()
    connect.close()
    return StockData

def GetStockList(): #读取中证1000成分股
    rd_excel = pd.read_excel(r'F:\IndexEnhance\000852closeweight.xlsx',dtype=object)
    InxList = rd_excel['成分券代码']
    return InxList

def GetAllStockList():  # 获得股票名单
     connect = pymysql.connect(host =  '192.168.20.200',
                                  user = 'root',
                                  password = 'ji666888',
                                 db = 'minerva_quotation',
                                  charset = 'utf8' 
                                  )
     cur = connect.cursor()
     print(cur)
     print('Getting stock list...')
     sql = "select table_name from information_schema.tables where table_schema = 'minerva_quotation'"
     df = pd.read_sql(sql,connect)  
        #df= df.values
        #dd = 'd_' in df.values.each()
        #df['table_ma,e'] = df['table_name'].astype(str)
     df=df.astype('string')
     dd = df.apply(lambda x: ('m1_0' in str(x)) or ('m1_3' in str(x))
                      or ('m1_6' in str(x)),axis=1)
        #dd = df.apply(lambda x: 'd_' == str(x)[0:2],axis=1)
     Stock_List = df[dd]
     Stock_List = np.array(Stock_List['table_name'])
     #Stock_List = np.array(Stock_List)
     # print('Stock_List: ', Stock_List[0:5])
     cur.close()
     connect.close()
     return Stock_List


def GetStockDayData(InxList):
    ### GetStockDayData ##############
    DayData = np.array([InxList,np.zeros(len(InxList))]).T
    #DayData.columns = ['Data']
    i=0
    print('Downloading Stock Day Data:')
    for stock in InxList:
        try:
            stockdata = DownloadDayData(stock)
            #stockdata = [stock,stockdata]
            DayData[i,1] = stockdata
            print('.',end='')
        except:
            print('Get ',stock,' Data Failed',end='')
        i += 1
    #################################
    return DayData

def GetInxDayData():
    stock = '399905' # 中证500 需要等待数据库补充中证1000的股票
    stockdata = DownloadDayData(stock)
    return stockdata

def ReshapeData(StockDayData,InxDayData):
    print('Reshaping Data..')
    stock_list = StockDayData[:,0]
    data_list = InxDayData['Time']
    data_list = np.array(data_list.apply(lambda x:str(x)[0:10]))
    #CloseMat = pd.DataFrame(index = stock_list)
    #CloseMat.columns = data_list
    #Mat = np.zeros(len(stock_list)*len(data_list))
    #Mat = Mat.reshape(len(data_list),len(stock_list))
    #Mat = pd.DataFrame(Mat,index = data_list)
    # Mat.columns = stock_list
    # Mat = pd.DataFrame(index = data_list)
    CloseMat = pd.DataFrame(index = data_list)
    OpenMat = pd.DataFrame(index = data_list)
    HighMat = pd.DataFrame(index = data_list)
    LowMat = pd.DataFrame(index = data_list)
    AmountMat = pd.DataFrame(index = data_list)
    for i in range(len(stock_list)):
        try:
            stock = stock_list[i]
            tmp = StockDayData[i,1]
            closeprice = tmp['ExClose']
            highprice = tmp['ExHigh']
            lowprice = tmp['ExLow']
            openprice = tmp['ExOpen']
            amount = tmp['Amount']
            stockdate = tmp['Time']
            stockdate = np.array(stockdate.apply(lambda x:str(x)[0:10]))
            closedata = pd.DataFrame(np.array(closeprice),index = stockdate)
            highdata =  pd.DataFrame(np.array(highprice),index = stockdate)
            lowdata = pd.DataFrame(np.array(lowprice),index = stockdate)
            opendata = pd.DataFrame(np.array(openprice),index = stockdate)
            amountdata = pd.DataFrame(np.array(amount),index = stockdate)
            closedata.columns = [stock]
            highdata.columns = [stock]
            lowdata.columns = [stock]
            opendata.columns = [stock]
            amountdata.columns = [stock]
            CloseMat = CloseMat.join(closedata)
            OpenMat = OpenMat.join(opendata)
            LowMat = LowMat.join(lowdata)
            HighMat = HighMat.join(highdata)
            AmountMat = AmountMat.join(amountdata)
        except:
            continue
    print('Reshaping Data Finished.')
    return  CloseMat,OpenMat,LowMat,HighMat,AmountMat



if __name__ == '__main__' :
    
    
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(10)

    T=True
    print('')
    print('Waiting DataBase.')
    while(T):
        AllTradingDate = GetTradingDate()
        td = datetime.datetime.now()
        td = td.date()
        EndDate = str(td)
        T = not(AllTradingDate[-1] == EndDate)
        print(AllTradingDate[-1])
        time.sleep(10)
    print('Start Running:')
    
    
    AllTradingDate = GetTradingDate()
    td = datetime.datetime.now()
    td = td.date()
    EndDate = str(td)

    StartDate = '2018-01-01'
    TradingDate = AllTradingDate[AllTradingDate>=StartDate]
    TradingDate = TradingDate[TradingDate<=EndDate]
    # InxList = GetStockList()
    StockList = pd.Series(GetAllStockList())
    StockList = StockList.apply(lambda x:x[3:9])
    StockDayData = GetStockDayData(StockList)
    DataDate = str(StockDayData[0][1]['Time'].iloc[-1])[0:10]
    InxDayData = GetInxDayData()
    
    ### 高波动股票池 ########################
    VolIndex = pd.Series(np.zeros(len(StockList)),index=StockList)
    for i in range(len(StockDayData)):
        try:
            stock = StockDayData[i][0]
            tmpData = StockDayData[i][1]
            ClosePrice = tmpData['Close']
            HighPrice = tmpData['High']
            LowPrice = tmpData['Low']
            amt = tmpData['Amount']
            rng = (HighPrice.iloc[-1]/LowPrice.iloc[-1] - 1)*100
            last_rtn = (ClosePrice.iloc[-1]/ClosePrice.iloc[-2] -1)*100
            last_amt = amt.iloc[-1]
            N_rtn = abs((ClosePrice.iloc[-1]/ClosePrice.iloc[-6] -1)*100)
            i_Index = (rng+last_rtn)*(rng+last_rtn)*last_amt*N_rtn
            VolIndex[stock] = i_Index
        except:
            continue
    VolIndex /= 100000000
    VolIndex.sort_values(ascending=False,inplace=True)
    TopList = VolIndex[VolIndex>300]
    TopList.sort_values(ascending=False,inplace=True)
    TopList.index.name = DataDate
    TopList.name = '活跃度'
    TopList.to_excel(r'HighVoliStocks\output\活跃度-%s.xlsx' %DataDate)

    
    ### 趋势选股 ########################################
    TrendIndex = pd.Series(np.zeros(len(StockList)),index=StockList)
    for i in range(len(StockDayData)):
        try:
            stock = StockDayData[i][0]
            tmpData = StockDayData[i][1]
            ClosePrice = tmpData['ExClose']
            HighPrice = tmpData['ExHigh']
            LowPrice = tmpData['ExLow']
            amt = tmpData['Amount']
            maxClose = ClosePrice[-60:].max()
            minClose = ClosePrice[-60:].min()
            ReToHigh = (maxClose/ClosePrice.iloc[-1] -1 )*100
            ReToLow = (ClosePrice.iloc[-1]/minClose -1)*100
            
            '''
            rng = (HighPrice.iloc[-1]/LowPrice.iloc[-1] - 1)*100
            last_rtn = (ClosePrice.iloc[-1]/ClosePrice.iloc[-2] -1)*100
            last_amt = amt.iloc[-1]
            N_rtn = abs((ClosePrice.iloc[-1]/ClosePrice.iloc[-6] -1)*100)
            i_Index = (rng+last_rtn)*(rng+last_rtn)*last_amt*N_rtn
            '''
            i_Index = int((ReToHigh<5) & (ReToLow>5))
            TrendIndex[stock] = i_Index
        except:
            continue
    TrendList = TrendIndex[TrendIndex>0]
    TrendList.index.name = DataDate
    TrendList.name = 'Mark'
    TrendList.to_excel(r'HighVoliStocks\output\TrendList-%s.xlsx' %DataDate)
















