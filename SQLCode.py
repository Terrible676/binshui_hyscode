# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 16:48:08 2022

@author: MC
"""
import os
import numpy as np
import scipy as sp
import tushare as ts
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime,date,timedelta
import pymysql


def GetData(Stock,DDate):
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    
    #sql = "select table_name from information_schema.tables where table_schema = 'minerva_quotation'"
    #df1 = pd.read_sql(sql,connect)
    #sql = "select column_name from information_schema.columns where table_schema = 'minerva_quotation' and table_name = 'm1_600426'"
    #df2 = pd.read_sql(sql,connect)
    #print(df1.loc[351:500])    # 0-350  capital_20220726
    #print(df2.loc[0:10])
    
        
    #    sql =  "select * from m1_600426 where Close>40 and Time> %s " % np.datetime64(Atime)  
    # s= np.datetime64(Atime)
    stock = 'm1_' + Stock
    #sql =  "select * from %s where Time > '%s'" %(stock,Atime)    # s= np.datetime64(Atime)
    sql =  "select * from %s where Time LIKE '%%%s%%' ORDER BY Time" %(stock,DDate)    # s= np.datetime64(Atime)LIKE '%%%s%%'"
    df = pd.read_sql(sql,connect)  #df3.columns df3.index.values
    df = GetPrice(df)             #print(df)
    
    cur.close()
    connect.close()
    return df
    
def GetPrice(df):
    df = df[["id","Amount","Close","High","Open","Low","Time"]]
    return df
    
def plotK(df):
    closeprice = df[['Close']]
    plt.plot(closeprice)
    return 0


'''
if __name__ == '__main__' :
    
    Stock = '600426'
    Atime = '2022-07-26 09:30:00'
    df = GetData(Stock,Atime)
    print(df)
    plotK(df)
    
    
    # d_000001 日线
    # m1_600519 1分钟线数据
'''
    
    
    