# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 14:50:54 2023

@author: MC
"""


import func1
import Strategy1
import pandas as pd
import numpy as np
import datetime
import os
import copy
import math
import matplotlib.pyplot as plt
import scipy.stats as stats
import time
import pymysql
from random import random

P1On = True
P2On = False
TurnNum = 1      #### 每分钟平均换手率，万一
MaxBuyTime = 1 

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


def GetIndexRet(index_data):
    zz500 = index_data[0][1]               # 399905 中证500深指当日股价 
    zz500_preDay = index_data[1].iloc[1]   # 中证500深指 前一日高开低收
    zz500_ExClose = zz500_preDay['Close']
    zz500 = zz500[1]
    close_zz500 = zz500['Close']
    ret_index = (close_zz500 /zz500_ExClose-1)*100  # 中证500开盘后的涨跌幅
    return ret_index

def weighted(output,adjr):
    w_output = copy.deepcopy(output)
    w_output.reset_index(drop=True, inplace=True)
    ll=len(w_output)
    for i in range(ll):
        ii = output.iloc[i]
        if(ii[3]<0):
            r = random()
            if(r<adjr):
                w_output.drop([i],axis=0,inplace=True)
    return w_output

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


#####价格支撑买入##############################
def isSupport(price,ExData,LastBuyPrice,Ret_index = 0,n=30):
    P1 = False
    try:
        ClosePrice = np.array(price['Close'])
        LowPrice =  np.array(price['Low'])
        HighPrice =  np.array(price['High'])
        OpenPrice =  np.array(price['Open'])
        ExClose = ExData['Close']
        ExVol = ExData['Vol']
        #ExLow = ExData['Low']
        ret = (ClosePrice[-1]/ExClose -1) * 100
        ret_rlt = ret - 2*Ret_index
        ret_cur =  (ClosePrice[-1]/OpenPrice[0] -1) * 100
        avp = func1.GetAVP(price)
        
        ##### DrawDown ################
        DrawDown = ( HighPrice.max()/ClosePrice[-1] -1) * 100
        if(LastBuyPrice<0):
            isMech = ClosePrice[-1]/avp[len(avp)-1] < 0.995
        else:
            isMech = ClosePrice[-1]/LastBuyPrice < 0.995
            
        if(not(isMech)):
            return False            
        if( DrawDown>2.5 ):
            return False
        # plunge
        nearAmount = price['Vol'][-3:].mean()
        plg = nearAmount/(price['Vol'].mean())
        if( plg > 1 ):
            return False
        ##########  GoldSpt#######################
        cp1 = list(HighPrice)
        cp2 = list(LowPrice)
        max_index = cp1.index(max(cp1))
        min_index = cp2.index(min(cp2))
        # GoldenRatio Support
        HH = HighPrice.max()
        LL = LowPrice.min()
        tmpLow = LowPrice[max_index:]
        tLL = tmpLow.min()
        Ratio1 = (HH -tLL)/max((HH -LL),0.1)   #防止出现0
        Ratio2 = (HH -ClosePrice[-1])/max((HH -LL),0.1)
        #Ratio3 = (HH/LL -1) *100
        GoldSpt = (max_index>min_index) and Ratio1<0.9 and Ratio2<0.8
        #######################################
        
        if(GoldSpt ):
            return True

    except:
        P1 = False
        return P1
    return P1
##########################################################

def SellHigh(price,ExData):
    ClosePrice = np.array(price['Close'])
    LowPrice =  np.array(price['Low'])
    HighPrice =  np.array(price['High'])
    OpenPrice =  np.array(price['Open'])
    ExClose = ExData['ExClose']
    ExVol = ExData['Vol']
    #ExLow = ExData['Low']
    ret = (ClosePrice[-1]/ExClose -1) * 100
    #ret_rlt = ret - 2*Ret_index
    #ret_cur =  (ClosePrice[-1]/OpenPrice[0] -1) * 100
    
    
    return



if __name__ == '__main__' :
    
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(10)


    ########### 交易日期 ######################################
    td = datetime.datetime.now()
    td = td.date()
    #DDate = '2023-03-31'
    DDate = str(td)
    # DDate = '2023-02-15'
    testNum = 6000  # 测试股票数量
    AllTradingDate = GetTradingDate()
    TradingDate = AllTradingDate[AllTradingDate<=DDate]
    preDate = TradingDate[-2] # 前一交易日日期
    pre2Date = TradingDate[-3] # 前2交易日日期
    ########################################################

    #### 持仓股票池和目标股票池 ######################
    List1 = pd.read_excel(r'HighVoliStocks\output\TrendList-%s.xlsx'%pre2Date,dtype=object)
    List2 = pd.read_excel(r'HighVoliStocks\output\TrendList-%s.xlsx'%preDate,dtype=object)
    List1 = List1.iloc[:,0]
    List2 = List2.iloc[:,0]
    BuyList = List2[~List2.isin(List1)]
    SellList = List1[~List1.isin(List2)]
    #print(BuyList)
    #print(SellList)
    ##############################################    

    
    
    ######### 买入交易股票池 ##############################
    stock_list = BuyList.apply(lambda x:'m1_'+x)
    stock_list2 = BuyList.apply(lambda x:'d_'+x)
    ######################################################
    ReadData = True                                        # 下载数据开关
    if ReadData:
        df_list = Strategy1.GetDataList(stock_list,DDate,testNum)
        DayData = Strategy1.GetDayData(stock_list2,preDate,testNum)
        index_data = Strategy1.GetIndexData(DDate,preDate)
        #print(df_list)
    
    ## for bar backtest ############
    BuyPoint = np.zeros(len(df_list),dtype = int)
    BuyPrice = np.zeros(len(df_list))
    BuyType = np.zeros(len(df_list),dtype = object)
    Rtn = np.zeros(len(df_list))
    StockName = np.zeros(len(df_list),dtype =object)
    ret_index = GetIndexRet(index_data)       # 获取中证500涨跌数据
    output1 = pd.DataFrame()
    PnL = 0; PnL1 = 0; PnL2= 0
    P1_Point = 0; P2_Point = 0
    TotalBuyPoint= 0 
    for i in range(len(df_list)):
        LastBuyPrice = -1  #第一个买点标记为-1
        BuyTimes = 0
        try:
            df = df_list[i]
            stock = df[0]
            StockName[i] = stock
            PriceData = df[1]
            ExData = DayData.loc[('d_'+stock[3:9])]
            for j in range(1,len(PriceData)):
                if(BuyTimes >= MaxBuyTime):
                    break
                tmpPrice = PriceData[0:j]
                tmp_index = ret_index[0:j]
                if(j>29 and j<235 and P1On):
                    P1 = isSupport(tmpPrice,ExData,LastBuyPrice,ret_index[j],)
                    if(P1):
                        BuyPrice[i] = PriceData['Open'][j+1]
                        if(BuyPrice[i]>0.1):
                            BuyPoint[i] = int(j)
                            BuyType[i] = 'P1'
                            LastBuyPrice = BuyPrice[i]
                            P1_Point += 1
                            Rtn[i] = ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                            PnL1 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                            BuyTimes += 1
                            TotalBuyPoint += 1
                            output1 = output1.append([[stock[3:9], BuyPoint[i], BuyPrice[i],Rtn[i]]])
                            print(stock, ' P1',j ,end=' ')
                        break
                if(j==235):
                    BuyPrice[i] = PriceData['Open'][j+1]
                    if(BuyPrice[i]>0.1):
                        BuyPoint[i] = int(j)
                        BuyType[i] = 'P1'
                        LastBuyPrice = BuyPrice[i]
                        P1_Point += 1
                        Rtn[i] = ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                        PnL1 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                        BuyTimes += 1
                        output1 = output1.append([[stock[3:9], BuyPoint[i], BuyPrice[i],Rtn[i]]])
                        print(stock, ' P1',j ,end=' ')
                        break
        except:
            continue
    PnL = PnL1+PnL2
    Total_Buy = len(BuyPoint[BuyPoint>0])
    PnL /= max(len(List1),1)
    output1.columns = (['买入代码','买入时间','买入价格','Rtn'])

    #############################################################
    output1 = output1[output1['买入时间']>0.1]
    #output2 = output1
    #output2 = weighted(output1,adjr=0.3)
    output1.reset_index(drop=True, inplace=True)
    adjPnL=output1['Rtn'].mean()

    #print("P2买入数量:",P2_Point,' 收益率:',round(PnL2/max(P2_Point,1),3),'%')

    BPTime = output1['买入时间']    #print("P1买入数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')

    BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
    output1['买入时间'] = BPTime.values
    output1 = output1[['买入代码','买入时间','买入价格']]
    #prt_output = copy.deepcopy(output)
    #prt_stock =  pd.Series(prt_output.index)
    #prt_output.index =prt_stock.apply(lambda x:x[3:9])
    #prt_output['买入时间']=BPTime.values
    #prt_output = prt_output[prt_output['买入价格']>0.1]
    os.makedirs(r'F:\hyscode\IntraDay\output', exist_ok=True)
    output1.to_excel('F:/hyscode/IntraDay/output/BuyPoint-%s-output.xlsx' %DDate,index=False)

########### 卖出 #############################################

    ######### 卖出交易股票池 ##############################
    stock_list = SellList.apply(lambda x:'m1_'+x)
    stock_list2 = SellList.apply(lambda x:'d_'+x)
    ##################################################
    ReadData = True                                        # 下载数据开关
    if ReadData:
        df_list = Strategy1.GetDataList(stock_list,DDate,testNum)
        DayData = Strategy1.GetDayData(stock_list2,preDate,testNum)
        index_data = Strategy1.GetIndexData(DDate,preDate)
        #print(df_list)
    
    ## for bar backtest ############
    SellPoint = np.zeros(len(df_list),dtype = int)
    SellPrice = np.zeros(len(df_list))
    SellType = np.zeros(len(df_list),dtype = object)
    Rtn = np.zeros(len(df_list))
    StockName = np.zeros(len(df_list),dtype =object)
    ret_index = GetIndexRet(index_data)       # 获取中证500涨跌数据
    output = pd.DataFrame()
    PnLSell = 0; PnL1 = 0; PnL2= 0
    P2_Point = 0
    TotalSellPoint= 0 
    for i in range(len(df_list)):
        try:
            df = df_list[i]
            stock = df[0]
            StockName[i] = stock
            PriceData = df[1]
            ExData = DayData.loc[('d_'+stock[3:9])]
            for j in range(1,len(PriceData)):
                tmpPrice = PriceData[0:j]
                tmp_index = ret_index[0:j]
                if(j==235):
                    SellPrice[i] = PriceData['Open'][j+1]
                    if(SellPrice[i]>0.1):
                        SellPoint[i] = int(j)
                        SellType[i] = 'P1'
                        LastSellPrice = SellPrice[i]
                        P2_Point += 1
                        Rtn[i] = (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
                        PnL2 += Rtn[i]
                        output = output.append([[stock[3:9], SellPoint[i], SellPrice[i],Rtn[i]]])
                        print(stock, ' P1',j ,end=' ')
                        break
        except:
            continue
    PnLSell = PnL1+PnL2
    Total_Sell = len(SellPoint[SellPoint>0])
    PnLSell /= max(len(List1),1)
    output.columns = (['卖出代码','卖出时间','卖出价格','Rtn'])

    #############################################################
    output2 = output[output['卖出时间']>0.1]
    #output2 = output1
    #output2 = weighted(output1,adjr=0.3)
    output2.reset_index(drop=True, inplace=True)
    adjPnL=output2['Rtn'].mean()
    
    ### 打印输出 ##############################
    print('')
    print(DDate,' 买入策略:')
    print('昨收股票数量:',len(List1))
    print('买入股票数量:',len(output1['买入代码'].unique()))
    #print('买入点数:',len(output2))
    #print('买入比例为：',round(Total_Buy/len(df_list)*100,3),'%')
    print("盘中买入数量:",P1_Point)
    print('买入平均收益率:',round(PnL*len(List1)/max(P1_Point,1),3),'%')
    print('增强收益率:',round(PnL,5),'%')
    print('')
    print(DDate,' 卖出策略:')
    print('今收股票数量',len(List2))
    print('卖出股票数量:',len(output2['卖出代码'].unique()))
    #print('买入点数:',len(output2))
    #print('卖出比例为：',round(Total_Sell/len(df_list)*100,3),'%')
    print("盘中卖出数量:",TotalSellPoint)
    print('卖出平均收益率:',round(PnLSell*len(List1)/len(output2['卖出代码'].unique()),3),'%')
    print('卖出增强收益率:',round(PnLSell,5),'%')
    
    ###################################
    BPTime = output2['卖出时间']
    BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
    output2['卖出时间'] = BPTime.values
    output2 = output2[['卖出代码','卖出时间','卖出价格']]
    #prt_output = copy.deepcopy(output)
    #prt_stock =  pd.Series(prt_output.index)
    #prt_output.index =prt_stock.apply(lambda x:x[3:9])
    #prt_output['买入时间']=BPTime.values
    #prt_output = prt_output[prt_output['买入价格']>0.1]
    os.makedirs(r'F:\hyscode\IntraDay\output', exist_ok=True)
    output2.to_excel('F:/hyscode/IntraDay/output/SellPoint-%s.xlsx' %DDate,index=False)

#### 全局输出 ############################################
    
    TradeData = pd.concat([List1,output1,output2,List2],axis=1)
    TradeCol = np.array(TradeData.columns)
    TradeCol[0] = '昨收持仓'
    TradeCol[1] = '买入代码'
    TradeCol[1] = '买入代码'
    TradeCol[-1] = '今收持仓'
    TradeData.columns=TradeCol
    TradeData.to_excel('F:/hyscode/IntraDay/output/IntraTrade-%s.xlsx' %DDate,index=False)
    
    #df2 = pd.Series(DataDict)
    #df2.name='交易统计'
    #result = pd.concat([ TradeData,df2],axis=1,ignore_index=False)
    #result.to_excel('F:/hyscode/IntraDay/output/IntraTrade-%s.xlsx' %DDate,index=False)
##########################################################################################
    
    
