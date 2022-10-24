# -*- coding: utf-8 -*-
"""
Created on Tue Aug 23 15:18:36 2022

@author: MC
"""
import tushare as ts
import pymysql
import pandas as pd

connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
cur = connect.cursor()
print(cur)
    
sql = "select table_name from information_schema.tables where table_schema = 'minerva_quotation'"
df1 = pd.read_sql(sql,connect)
    #sql = "select column_name from information_schema.columns where table_schema = 'minerva_quotation' and table_name = 'm1_600426'"
    #df2 = pd.read_sql(sql,connect)
    #print(df1.loc[351:500])    # 0-350  capital_20220726
    #print(df2.loc[0:10])
    
        
    #    sql =  "select * from m1_600426 where Close>40 and Time> %s " % np.datetime64(Atime)  
    # s= np.datetime64(Atime)
DDate = '2022-8-22'
Stock = '000001'
stock = 'm1_' + Stock
   #sql =  "select * from %s where Time > '%s'" %(stock,Atime)    # s= np.datetime64(Atime)
sql =  "select * from %s where Time LIKE '%%%s%%' ORDER BY Time" %(stock,DDate)    # s= np.datetime64(Atime)LIKE '%%%s%%'"
df = pd.read_sql(sql,connect)  #df3.columns df3.index.values
    
cur.close()
connect.close()
