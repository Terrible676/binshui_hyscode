# -*- coding: utf-8 -*-
"""
Created on Thu Sep  8 14:49:46 2022

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
MaxBuyTime =3 

def GetIndexRet(index_data):
    zz500 = index_data[0][1]               # 399905 中证500深指当日股价 
    zz500_preDay = index_data[1].iloc[1]   # 中证500深指 前一日高开低收
    zz500_ExClose = zz500_preDay['Close']
    zz500 = zz500[1]
    close_zz500 = zz500['Close']
    ret_index = (close_zz500 /zz500_ExClose-1)*100  # 中证500开盘后的涨跌幅
    return ret_index


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
        '''
        ##### 支撑价 ###############
        Supporting = ( ClosePrice[-1]/LowPrice.min() -1) * 100
        Speed_Sup = Supporting/ (max(len(ClosePrice)/10,1))
        ##############################
        '''
        
        ##### DrawDown ################
        DrawDown = ( HighPrice.max()/ClosePrice[-1] -1) * 100
        
        #移动最大值：
        #eHH = pd.Series(HighPrice).expanding().max()
        #eLL = pd.Series(LowPrice).expanding().min()
        #epd = (eHH / eLL -1) *100
        #Diff_Num = (ClosePrice / pd.Series(ClosePrice).shift(1) -1)*100
        #Diff_Num[0] = 0
        #acf = smt.stattools.acf(Diff_Num)
        
        # ##### 强势冲高买入 ############
        # if(ret_rlt> 4 and ret_cur>1 and Supporting > 2 and Speed_Sup>1):
        #     P1 = True             
        #     return P1
        # ######################
        
        
        ##### 成交量换手数据  ########################
        '''
        dif = (ClosePrice / avp -1) * 100
        cur_ret = (ClosePrice[-1] / ClosePrice[0] -1) * 100
        AboveAVP = dif[dif>0]
        AboveRatio = len(AboveAVP)/len(ClosePrice)
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        ####################################################
        ''' 
        
        # print(TurnOver)
        # isBoom1 = (avg_Vol*240*(max(-cur_ret,1)) / ExVol > 10)
        # 多点低吸过滤
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

        '''
        if(GoldSpt and AboveRatio > 0.1 and avg_Amount>10 and TurnOver>0.2 and isMech):
            P1 = True
            return P1
        '''
    except:
        P1 = False
        return P1
    return P1
##########################################################




# def isPlunge(price,ExData):
    
    
'''
#####################################################
def GetNextDayRet(stock_list2,output,DDate,testNum):   # 计算第二天收益率
    #### 计算相对收益率 ###################
    dd = datetime.datetime.strptime(DDate, "%Y-%m-%d")
    wd = dd.weekday()
    if(wd==4):
        NextDate = dd + datetime.timedelta(days=3)
    else:
        NextDate = dd + datetime.timedelta(days=1)                               # 前一天数据
    NextDate = str(NextDate)[0:10]
    ############################################
    NextData = Strategy1.GetDayData(stock_list2,NextDate,testNum)
    # buylist = output[output['买入时间']>0.1]
    NextDayReturn = np.zeros(len(output))
    for i in range(len(output)):
        try:
            stock = output.index[i]
            stockD = 'd_'+stock[3:9]
            NextClose = NextData.loc[stockD]['Close']
            
            NextDayReturn[i] = (NextClose/(output['买入价格'][i]) -1)*100
        except:
            continue
    return NextDayReturn
################################################################
'''


if __name__ == '__main__' :
    
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(10)

    ########### 获取股票池 ######################################
    td = datetime.datetime.now()
    td = td.date()
    #DDate = '2023-02-21'
    DDate = str(td)
    testNum = 6000  # 测试股票数量
    # DDate = '2023-02-15'
    AllStockList = pd.Series(GetAllStockList())
    
    StockList = AllStockList[:testNum]
    #StockList = pd.read_excel(r'F:\hyscode\HighVoliStocks\output\TrendList-2023-03-24.xlsx',dtype=object)
    #stock_list = StockList[StockList.columns[0]]
    #stock_list = stock_list.apply(lambda x:'m1_'+x)
    stock_list = StockList
    stock_list2 = stock_list.apply(lambda x:'d_'+(x[3:9]))
    
    #### 获取前一交易日日期#############################
    dd = datetime.datetime.strptime(DDate, "%Y-%m-%d")
    wd = dd.weekday()
    if(wd==0):
        preDate = dd -datetime.timedelta(days=3)
    else:
        preDate = dd -datetime.timedelta(days=1)                               # 前一天数据
    preDate = str(preDate)[0:10]
    ############################################
    
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
    output = pd.DataFrame()
    PnL = 0; PnL1 = 0; PnL2= 0
    P1_Point = 0; P2_Point = 0
    for i in range(len(df_list)):
        LastBuyPrice = -1  #第一个买点标记为-1
        BuyTimes = 0
        try:
            df = df_list[i]
            stock = df[0]
            StockName[i] = stock
            PriceData = df[1]
            ExData = DayData.loc[('d_'+stock[3:9])]
            '''
            try:
                Num = np.argwhere(np.array(CapitalData['代码'])==stock[3:9])[0][0]
                Capital = CapitalData.iloc[Num][1]
            except:
                Capital = 10000000000 # 没有数据则以100亿计算
            '''
            ## 去掉低价股 ####
            if(ExData['ExClose']<3):
                continue
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
                            output = output.append([[stock[3:9], BuyPoint[i], BuyPrice[i],Rtn[i]]])
                            print(stock, ' P1',j ,end=' ')
                '''
                if(j>180 and j<234 and P2On):
                    P2 = Diver2Index(tmpPrice,tmp_index,Capital)  # 与大盘背离
                    if(P2):
                        BuyPrice[i] = PriceData['Open'][j+1]
                        if(BuyPrice[i]>0.1):
                            BuyPoint[i] = int(j)
                            BuyType[i] = 'P2'
                            LastBuyPrice = BuyPrice[i]
                            P2_Point +=1
                            BuyTimes += 1
                            PnL2 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                            output = output.append([[stock, BuyPoint[i], BuyPrice[i]]])
                            print(stock,' P2',j)
                '''
        except:
            continue
            
    #print(BuyPoint)
    PnL = PnL1+PnL2
    Total_Buy = len(BuyPoint[BuyPoint>0])
    PnL /= max(Total_Buy,1)
    #output = pd.DataFrame([BuyPoint,BuyPrice,BuyType]).T#,columns=['卖出时间','卖出价格'])
    #output.index = StockName
    output.columns = (['股票代码','买入时间','买入价格','Rtn'])
    
    ##### 下一交易日收盘收益率  #################################
    #output = output[output['买入时间']>0.1]
    #NextRet = GetNextDayRet(stock_list2,output,DDate,testNum)
    
    
    
    #############################################################
    output1 = output[output['买入时间']>0.1]
    output2 = output1
    #output2 = weighted(output1,adjr=0)
    output2.reset_index(drop=True, inplace=True)
    adjPnL=output2['Rtn'].mean()
    print('')
    print('adjPnL:',adjPnL)
    print(DDate,' 低吸策略:')
    print('总股票数量：',len(df_list))
    print('买入股票数量:',len(output2['股票代码'].unique()))
    print('买入点数:',len(output2))
    print('买入比例为：',round(Total_Buy/len(df_list)*100,3),'%')
    print("P1买入数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')
    print("P2买入数量:",P2_Point,' 收益率:',round(PnL2/max(P2_Point,1),3),'%')

    BPTime = output2['买入时间']
    BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
    output2['买入时间'] = BPTime.values
    output2 = output2[['股票代码','买入时间','买入价格']]
    #prt_output = copy.deepcopy(output)
    #prt_stock =  pd.Series(prt_output.index)
    #prt_output.index =prt_stock.apply(lambda x:x[3:9])
    #prt_output['买入时间']=BPTime.values
    #prt_output = prt_output[prt_output['买入价格']>0.1]
    os.makedirs(r'F:\hyscode\NewBuyLow\output', exist_ok=True)
    output2.to_excel('F:/hyscode/NewBuyLow/output/多点低吸7-%s-output.xlsx' %DDate,index=False)

    








