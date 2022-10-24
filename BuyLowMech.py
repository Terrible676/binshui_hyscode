# -*- coding: utf-8 -*-
"""
Created on Thu Sep 22 10:43:58 2022
早盘机械低吸
@author: MC
"""

import Strategy1
import pandas as pd
import numpy as np
import datetime
import os
import copy
import math
import matplotlib.pyplot as plt
import scipy.stats as stats
import pymysql
from alive_progress import alive_bar
import time

def GetAVP(priceData):               # 计算日内均价
    Amount = priceData["Amount"]
    #ClosePrice = priceData["Close"]
    Volume = priceData['Vol']
    AVP = 0.01* Amount.cumsum()/Volume.cumsum()
    return AVP


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
    Df_list = np.array([Trading_List,Trading_List]).T

    i=0
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
##########################################################

def isSupport(price,ExData):
    try:
        ClosePrice = np.array(price['Close'])
        HighPrice = np.array(price['High'])
        ExClose = ExData['Close']
        cur_ret = (ClosePrice[-1]/ExClose -1) *100
        HH = HighPrice.max()
        draw_speed = (HH / ClosePrice[-1] -1) *100 /len(ClosePrice) * 30 
        if(cur_ret>-4 and draw_speed > -1.5):
            return True
        else:
            return False
    except:
        return True
    return True
        
    
##### isMech #################################
def isMech(price):
    try:
        ClosePrice = np.array(price['Close'])
        avp = GetAVP(price)
        dif = (ClosePrice / avp -1) * 100

        if(dif[len(dif)-1]<-0.5  and dif[len(dif)-2]<-0.5):
            return True
        else:
            return False
        # print(TurnOver)
        #############################################
    except:
        return False
    return False
##########################################################

def isPunch(price,ExData):   # 冲高回落去除
    try:
        HighPrice = np.array(price['High'])
        ExClose = ExData['Close']
        punch = (HighPrice.max() /ExClose -1)*100
        if( punch>4 ):
            return True
        else:
            return False
    except:
        return False
    return False

if __name__ == '__main__' :
    
    
        
    ##### 等待4点半开始运行 #####################
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(2)
    #########################################

    rd_excel = pd.read_excel(r'F:/hyscode/RealStockList1013.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    rd_j = rd_excel['代码']
    stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8])) 
    stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    
    
    ########### 下载数据 ############################################
    BTDate =['2022-10-14']
    for DDate in BTDate:
        testNum = 3000
        ReadData = True                                        # 下载数据开关
        #### 获取前一交易日日期#############################
        dd = datetime.datetime.strptime(DDate, "%Y-%m-%d")
        wd = dd.weekday()
        if(wd==0):
            preDate = dd -datetime.timedelta(days=3)
        else:
            preDate = dd -datetime.timedelta(days=1)                               # 前一天数据
        preDate = str(preDate)[0:10]
        ############# 下载数据  ###################
        if ReadData:
            df_list = GetDataList(stock_list,DDate,testNum)
            DayData = GetDayData(stock_list2,preDate,testNum)
        ##################################################
            
        ## for bar backtest ############
        BuyPoint = np.zeros(len(df_list),dtype = int)
        BuyPrice = np.zeros(len(df_list))
        BuyType = np.zeros(len(df_list),dtype = object)
        FirstPoint = np.zeros(len(df_list),dtype = int)
        BuyTimes = np.zeros(len(df_list),dtype = int)
        FirstPrice = np.zeros(len(df_list))
        StockName = np.zeros(len(df_list),dtype =object)
        PnL = 0; PnL1 = 0; 
        P1_Point = 0
        Total_Point = 0
        for i in range(len(df_list)):
            try:
                df = df_list[i]
                stock = df[0]
                StockName[i] = stock
                PriceData = df[1]
                ExData = DayData.loc[('d_'+stock[3:9])]
                for j in range(30,235):
                    P1 =  False
                    tmpPrice = PriceData[0:j+1]   #j+1
                    Pch = isPunch(tmpPrice,ExData)  # 冲高回落不买
                    if Pch:
                        break
                    P1 = isMech(tmpPrice) and isSupport(tmpPrice,ExData)
                    if(P1):
                        BuyPoint[i] = int(j)
                        BuyPrice[i] = PriceData['Open'][j+1]
                        if(BuyTimes[i]==0):
                            FirstPoint[i] = int(j)
                            FirstPrice[i] = PriceData['Open'][j+1]
                            PnL1 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                            P1_Point += 1
                        BuyTimes[i] += 1
                        Total_Point += 1
                        #print(stock,' P1',j)
            except:
                continue
                
        ######  计算收益率 ##################################
        PnL = PnL1
        Total_Sold = len(BuyPoint[BuyPoint>0])
        PnL /= max(Total_Sold,1)
        output = pd.DataFrame([FirstPoint,FirstPrice,BuyPoint,BuyPrice,BuyTimes]).T#,columns=['买入时间','买入价格'])
        output.index = StockName
        output.columns = (['买入时间','买入价格','最后买入时间','最后买入价格','买入次数'])
        
        print(DDate,' 早盘持续给点低吸')
        print('总股票数量：',len(df_list))
        print('买入股票数量:',Total_Sold)
        print('给点次数:',Total_Point)
        print('平均收益率为：',round(PnL,3),'%')
        print('买入比例为：',round(Total_Sold/len(df_list)*100,3),'%')
        print("P1数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')                
        BPTime = output['买入时间']
        BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        LastTime = output['最后买入时间']
        LastTime = LastTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        prt_output = copy.deepcopy(output)
        prt_stock =  pd.Series(prt_output.index)
        prt_output.index =prt_stock.apply(lambda x:x[3:9])
        prt_output['买入时间']=BPTime.values
        prt_output['最后买入时间']=LastTime.values
        prt_output = prt_output[prt_output['买入价格']>0.1]
        os.makedirs(r'F:\hyscode\BuyLowMech\output', exist_ok=True)
        prt_output.to_excel('F:/hyscode/BuyLowMech/output/重复给点低吸%s output.xlsx' %DDate)

                
        ##################################################


        
        
        
        
        
        
        




