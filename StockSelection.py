# -*- coding: utf-8 -*-
"""
Created on Tue Sep 13 14:08:23 2022

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
import Strategy1



def GetStockList():  # 获得股票名单
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
     print('Stock_List: ', Stock_List[0:5])
     cur.close()
     connect.close()
     return Stock_List
 
def GetDayData(Stock_List,DDate,TestNum = 100):
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    Trading_List = np.array(Stock_List[0:TestNum])
    #df_list = pd.DataFrame(np.zeros([len(Trading_List),1]),index=Trading_List)
    #df_list = np.zeros([len(Test_List),1])
    # Df_list = np.array([Trading_List,Trading_List]).T
    Df_list = pd.DataFrame()
    #df_list = pd.DataFrame()
    #    Atime = '2022-07-26 09:30:00'

    i=0
    #Atime = DDate+' 10:22:00'
    #Atime = Timestamp(Atime)
    #Atime = np.datetime64(Atime)
    #Atime = datetime.datetime.strptime(Atime, '%Y-%m-%d %H:%M:%S')
    for stock in Trading_List:
        try:
            sql =  "select * from %s where Time LIKe '%%%s%%' ORDER BY Time" %(stock,DDate)
            #Df_list[i,1] = pd.read_sql(sql,connect)
            readsql = pd.Series(pd.read_sql(sql,connect).values[0],name=stock)
            Df_list = Df_list.append(readsql)
            i = i + 1
            # print(i,' ',stock,'  ',end="")
        except:
            i = i + 1
            print("No Stock Day Data")
            continue
    #df_list = pd.Series(df_list,index=Trading_List)
    #df_list = pd.DataFrame(Df_list[:,1],index=Df_list[:,0])
    Df_list.columns = ['id','Amount','Open','ExOpen','Close','ExClose','High','ExHigh','Low','ExLow','Time','Vol','CreateTime','UpdateTime']
    dayData = Df_list
    cur.close()
    connect.close()
    return dayData


def DownDayData(StockList,StartDate = '2018-01-01',EndDate='2022-09-01',TestNum=100):
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    print('Downloading Day Data....')
    sql = "select column_name from information_schema.columns where table_schema = 'minerva_quotation'"
    #DayListData = pd.DataFrame()
    dict_stock = {}  # 字典类型
    with alive_bar(TestNum) as bar:   ## 显示读取数据进度
        for stock in StockList[0:TestNum]:
            bar()
            try:
                # sql =  "select * from %s where Time > '%%%s%%' ORDER BY Time" %(stock,StartDate)
                sql =  "select * from %s ORDER BY Time" %(stock)
                readsql = pd.read_sql(sql,connect)
                #DayListData =DayListData.append(readsql)
                dict_stock[stock] = readsql
            except:
                print('No',stock,' day data')

    cur.close()
    connect.close()

    return dict_stock

def ShortTermPattern(StockData):
    
    return

def LongTermPattern(StockData):
    
    return


def GetCloseMatrix(dict_stock,N=750):
    CloseMatrix = pd.DataFrame()
    for stock in dict_stock:
        try:
            closeprice = np.array(dict_stock[stock]['Close'])
            cp_N = closeprice[-N:]
            CloseMatrix = CloseMatrix.append([cp_N],index = stock)
        except:
            continue
    return


if __name__ == '__main__':
    print('1')
    #rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0909.xlsx',dtype=object)
    #rd_j = rd_excel['代码']
    slist = pd.Series(Strategy1.GetStockList())
    stock_list = slist.apply(lambda x:'d_'+(x[3:9]))
    #stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    StartDate = '2022-05-01'
    EndDate = '2022-09-09'
    TestNum = 100
    ReadData = True                                        # 下载数据开关
    if ReadData:
        dict_stock = DownDayData(stock_list,StartDate,EndDate,TestNum)
        
    
    
    
    for stock in dict_stock:
        #print(stock)
        stockData = dict_stock[stock]
        ClosePrice = stockData['Close']
        
        
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    