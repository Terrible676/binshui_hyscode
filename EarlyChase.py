# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 14:56:58 2023

@author: MC
"""


import Strategy1
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
import threading




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
        sql = 'select * from d_%s ORDER BY Time'%stock
        readsql = pd.read_sql(sql,connect)
        StockData = readsql
    except:
        print('Donwload Stock Data Failed')
    cur.close()
    connect.close()
    return StockData

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
     df=df.astype('string')
     dd = df.apply(lambda x: ('m1_0' in str(x)) or ('m1_3' in str(x))
                      or ('m1_6' in str(x)),axis=1)
        #dd = df.apply(lambda x: 'd_' == str(x)[0:2],axis=1)
     Stock_List = df[dd]
     Stock_List = np.array(Stock_List['table_name'])
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
    Df_list = pd.DataFrame()
    i=0
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
            print("No Stock Day Data ",end='')
            continue
    #df_list = pd.Series(df_list,index=Trading_List)
    #df_list = pd.DataFrame(Df_list[:,1],index=Df_list[:,0])
    Df_list.columns = ['id','Amount','Open','ExOpen','Close','ExClose','High','ExHigh','Low','ExLow','Time','Vol','CreateTime','UpdateTime']
    dayData = Df_list
    cur.close()
    connect.close()
    return dayData
##########################################################

def GetIndexData(DDate,preDate):  # 获取指数行情
    # 399001 深证成指
    # 399905 中证500
    # 399005 中小100
    # 399006 创业板指
    # 399300 沪深300
    IndexCode = pd.Series(['399001','399905','399005','399006','399300'])
    index_list = IndexCode.apply(lambda x:'m1_'+x)
    index_list2 = IndexCode.apply(lambda x:'d_'+x)
    df = GetDataList(index_list,DDate)
    DayData = GetDayData(index_list2,preDate)
    return df,DayData


def GetIndexRet(index_data):
    zz500 = index_data[0][1]               # 399905 中证500深指当日股价 
    zz500_preDay = index_data[1].iloc[1]   # 中证500深指 前一日高开低收
    zz500_ExClose = zz500_preDay['Close']
    zz500 = zz500[1]
    close_zz500 = zz500['Close']
    ret_index = (close_zz500 /zz500_ExClose-1)*100  # 中证500开盘后的涨跌幅
    return ret_index


##### 放量上涨 ##############################
def isPumpDown(price,ExData,Capital,Ret_index = 0):
    P1 = False
    try:
        ClosePrice = np.array(price['Close'])
        #LowPrice =  np.array(price['Low'])
        HighPrice =  np.array(price['High'])
        OpenPrice =  np.array(price['Open'])
        ExClose = ExData['Close']
        ExVol = ExData['Vol']
        #ExLow = ExData['Low']
        ret = (ClosePrice[-1]/ExClose -1) * 100
        #ret_rlt = ret - 2*Ret_index
        ret_rlt = ret - 2*np.sign(Ret_index)*min(abs(Ret_index),1)
        DrawDown = (HighPrice.max() / ClosePrice[-1] -1) * 100
        
        
        
        cur_ret = (ClosePrice[-1] / OpenPrice[0] -1) * 100
        open_ret = (OpenPrice[0] /ExClose -1)*100
        # BelowAVP = dif[dif<0]
        # BelowRatio = len(BelowAVP)/len(ClosePrice)
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        # print(TurnOver)
        if(ExVol < 1):
            return False
        isPump1 = (avg_Vol*240*(max(cur_ret,1)) / ExVol > 10)
        
        if( open_ret>-1 and
           open_ret< 4 and 
            (cur_ret >1) and   #(len(ClosePrice)>5) and 
            cur_ret< 3.5 and 
           (ret_rlt>0.5) and
           DrawDown < 1 and
           avg_Amount > 100 and   #平均成交量大于100万
           TurnOver > 0.5 and        
           isPump1
           ):
            P1 = True
            return P1
        ##########################################
    except:
        P1 = False
        return P1
    return P1
##########################################################


def GetStockDayData(StockList):
    ### GetStockDayData ##############
    DayData = np.array([StockList,np.zeros(len(StockList))]).T
    #DayData.columns = ['Data']
    i=0
    print('Downloading Stock Day Data:')
    for stock in StockList:
        try:
            stockdata = DownloadDayData(stock)
            #stockdata = [stock,stockdata]
            DayData[i,1] = stockdata
            print('.',end='')
        except:
            print('Get ',stock,' Data Failed',end='')
        i += 1
    #################################
    print('Downlaoding Stock Day Data Finished.')
    return DayData

def GetInxDayData():
    stock = '399905' # 中证500 需要等待数据库补充中证1000的股票
    stockdata = DownloadDayData(stock)
    return stockdata

def GetDataList(Stock_List,DDate,TestNum=100):
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    print('Getting Data List')
    Trading_List = np.array(Stock_List[0:TestNum])
    #df_list = pd.DataFrame(np.zeros([len(Trading_List),1]),index=Trading_List)
    #df_list = np.zeros([len(Test_List),1])
    Df_list = np.array([Trading_List,Trading_List]).T
    #df_list = pd.DataFrame()
    #    Atime = '2022-07-26 09:30:00'

    i=0
    #Atime = DDate+' 10:22:00'
    #Atime = Timestamp(Atime)
    #Atime = np.datetime64(Atime)
    #Atime = datetime.datetime.strptime(Atime, '%Y-%m-%d %H:%M:%S')
    with alive_bar(len(Trading_List)) as bar:   ## 显示读取数据进度
        for stock in Trading_List:
            try:
                sql =  "select * from %s where Time LIKe '%%%s%%' ORDER BY Time" %(stock,DDate)
                Df_list[i,1] = pd.read_sql(sql,connect)
                i = i + 1
                # print(i,' ',stock,'  ',end="")
                bar()
            except:
                bar()
                i = i + 1
                print("No Stock Data")
                continue
    #df_list = pd.Series(df_list,index=Trading_List)
    #df_list = pd.DataFrame(Df_list[:,1],index=Df_list[:,0])
    df_list = Df_list
    cur.close()
    connect.close()
    return df_list

if __name__ == '__main__' :
    
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(10)
        
    AllTradingDate = GetTradingDate()
    td = datetime.datetime.now()
    td = td.date()

    StartDate = '2022-08-01'
    #EndDate = '2023-02-27'
    EndDate = str(td)
    testNum = 6000  # 测试股票数量

    TradingDate = AllTradingDate[AllTradingDate>=StartDate]
    TradingDate = TradingDate[TradingDate<=EndDate]
    DDate = EndDate
    LastDate = EndDate
    preDate = TradingDate[-2]

    AllStockList = pd.Series(GetAllStockList())
    InxDayData = GetInxDayData()
    StockList = AllStockList[:testNum]
    StockList = StockList.apply(lambda x:x[3:9])  

    ### Last Day Data ###################
    # DDate = EndDate
    mStockList = StockList.apply(lambda x:'m1_'+x)
    df_list = GetDataList(mStockList,LastDate,6000)  ## 最后一日分钟数据
    stock_list2 = mStockList.apply(lambda x:'d_'+(x[3:9]))
    DayData = GetDayData(stock_list2,preDate,testNum)
    index_data = GetIndexData(LastDate,preDate)
    
    ##### 读取市值数据  ########################################################
    read_Capital = pd.read_excel(r'F:/hyscode/CapitalData2.xlsx',dtype=object)
    dm = read_Capital['代码']
    dm = dm.apply(lambda x:x[2:8])
    CapitalData = pd.DataFrame([dm,read_Capital['总市值']]).T
    ########### 下载数据 ############################################

    ## for bar backtest ############
    BuyPoint = np.zeros(len(df_list),dtype = int)
    SellPrice = np.zeros(len(df_list))
    SellType = np.zeros(len(df_list),dtype = object)
    StockName = np.zeros(len(df_list),dtype =object)
    ret_index = GetIndexRet(index_data)       # 获取中证500涨跌数据
    PnL = 0; PnL1 = 0; PnL2= 0
    P1_Point = 0; P2_Point = 0
    for i in range(len(df_list)):
        try:
            df = df_list[i]
            stock = df[0]
            StockName[i] = stock
            PriceData = df[1]
            ExData = DayData.loc[('d_'+stock[3:9])]
            #DayHigh = LastMax[i]
            #DayLow = LastMin[i]
            try:
                Num = np.argwhere(np.array(CapitalData['代码'])==stock[3:9])[0][0]
                Capital = CapitalData.iloc[Num][1]
            except:
                Capital = 10000000000 # 没有数据则以100亿计算
            ## 低价股过滤 ####
            if(ExData['ExClose']<3):
                print('低价股',end='')
                continue
            for j in range(2,20):
                tmpPrice = PriceData[0:j+1]   #j+1
                tmp_index = ret_index[0:j+1]
                P1 = isPumpDown(tmpPrice,ExData,Capital,ret_index[j]) # and isMech(tmpPrice,ExData,Capital,ret_index[j])
                if(P1):
                    BuyPoint[i] = int(j)
                    SellPrice[i] = PriceData['Open'][j+1]
                    SellType[i] = 'P1'
                    P1_Point += 1
                    PnL1 += ((np.array(PriceData['Close'])[-1])/SellPrice[i] -1)*100
                    print(stock,' P1',j,' ',end='')
                    break
        except:
            continue
    PnL = PnL1+PnL2
    Total_Sold = len(BuyPoint[BuyPoint>0])
    PnL /= max(Total_Sold,1)
    output = pd.DataFrame([BuyPoint,SellPrice,SellType]).T#,columns=['买入时间','买入价格'])
    output.index = StockName
    output.columns = (['买入时间','买入价格','买入类型'])
    
    print(DDate,' 早盘追:')
    print('总股票数量：',len(df_list))
    print('买入股票数量:',Total_Sold)
    print('平均收益率为：',round(PnL,3),'%')
    print('买入比例为：',round(Total_Sold/len(df_list)*100,3),'%')
    print("P1数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')
    print("P2数量:",P2_Point,' 收益率:',round(PnL2/max(P2_Point,1),3),'%')
    
    
    
    BPTime = output['买入时间']
    BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
    prt_output = copy.deepcopy(output)
    prt_stock =  pd.Series(prt_output.index)
    prt_output.index =prt_stock.apply(lambda x:x[3:9])
    prt_output['买入时间']=BPTime.values
    prt_output = prt_output[prt_output['买入价格']>0.1]
    os.makedirs(r'F:\hyscode\EarlyChase\output', exist_ok=True)
    prt_output.to_excel('F:/hyscode/EarlyChase/output/EarlyChase-%s output.xlsx' %DDate)

    # mClose,mOpen,mLow,mHigh,mAmount = ReshapeMinData(minData)  ### 分钟数据




