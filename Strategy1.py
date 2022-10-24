# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 11:14:29 2022

@author: MC
"""


import SQLCode
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



RBreakOn = False

LowVolOn = True
HighVolDelay = True
weightOn = True
MechDelay = True
NosignMechOn1 = False
NosignMechOn2 = True
LinearDownOn = True

'''
def BuyLow(price):
    ClosePrice = func1.GetClosePrice(price)
    HighPrice = func1.GetHighPrice(price)
    return
'''



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
     '''
     """
       # 涉及到多表查询的时候 字段名称容易冲突 需要使用表名点字段的方式区分
       # inner join:只拼接两张表中共有的部分
       select * from emp inner join dep on emp.dep_id = dep.id;
       # left join:以左表为基准展示所有的内容 没有的NULL填充
       select * from emp left join dep on emp.dep_id = dep.id;
       # right join:以右表为基准展示所有的内容 没有的NULL填充
       select * from emp right join dep on emp.dep_id = dep.id;
       # union:左右表所有的数据都在 没有的NULL填充
       select * from emp left join dep on emp.dep_id = dep.id
       union
       select * from emp right join dep on emp.dep_id = dep.id;
       """
     '''
     return Stock_List

'''
def BuyLowTrade(df,BP,pnl):
    if(BP<(len(df)-1)):
        BuyPrice = df['Open'].iloc[BP[0]+1]
        TradeReturn = (df['Close'].iloc[-1] - BuyPrice)/BuyPrice
        PnL = pnl + TradeReturn
    print(pnl)
    return PnL
'''

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
    
##### 高抛策略，把股价数据反转 ##############
def DataReverse(DfList):
    #dd = df_list
    RevData = copy.deepcopy(DfList)
    j=0
    for i in range(len(DfList)):
        ## 反转 data -> dd_data
        ##dd = -dd +2*dd['Open',0] ## 取负加两倍开盘价
        try:
            #dd = np.array(DfList[i])
            dd = copy.deepcopy(DfList[i])
            #if i==10:    
            #    plt.plot(dd[1]['Close'],color = 'blue')
            dd_data = pd.DataFrame(dd[1])   ###数值会被修改
            d_open = float(1.* (dd_data['Open'][0]))  ### 数值不会被修改
            dd_data['Open'] = 1.*(pd.Series(-dd_data['Open'] + 2*d_open))
            dd_data['High'] = 1.*(pd.Series(-dd_data['High'] + 2*d_open))
            dd_data['Close'] = 1.*(pd.Series(- dd_data['Close'] + 2*d_open))
            dd_data['Low'] = 1.*(pd.Series(-dd_data['Low'] + 2*d_open))
            #if i==10:
            #    plt.plot(dd_data['Close'],color = 'orange')    
            # 连接起来
            rev_data = np.array([dd[0],dd_data],dtype=object)
            RevData[i] = np.array(rev_data)
        except:
            print('Reversing df_list %s'%i,'failed')
            j += 1
            continue
    print('%s'%j,' stocks failed in total')
    if(len(RevData) == len(DfList)):
        print('Data Reversed Succeed.')
    return RevData
########################################

def GetWeight():
    #weightData = pd.read_excel('weightData.xlsx',dtype=object)
    weightData = pd.read_excel('低吸金额.xlsx',dtype=object)
    print('Getting Weight Data Succeed.')
    weightData = pd.DataFrame(weightData[['代码','低吸']])
    dm = weightData['代码']
    dm = dm.apply(lambda x:(x[2:8]))
    weightData['代码'] = dm
    return weightData


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

def GetBPTime(BP=0):
    ########## 调整时间输出格式
    TradeTime = 0
    tt = BP
    if(BP>120):
        tt = BP + 90
    if(BP>0):
        OpenTime = '9:30'
        OpenTime = datetime.datetime.strptime(OpenTime,'%H:%M')
        TradeTime = OpenTime + datetime.timedelta(minutes=tt)
        TradeTime = TradeTime.strftime('%H:%M')
    #time.strftime("%H:%M:%S", time.localtime())
    return TradeTime



###### 买点 直线下跌过滤 #####################################################
def LinearDownFilter(df_list):
    df = []
    for i in range(len(df_list)):
        try:
            df = copy.deepcopy(df_list[i,1])
        except:
            continue
        if(len(df)>0):
            #d_open = df['Open'].iloc[0]
            closeprice = df['Close']
            AVP = func1.GetAVP(df)
            dif = closeprice - AVP
            '''
            plt.plot(closeprice)
            plt.plot(AVP)
            '''
    return
####################################################################

##### 创新低低吸过滤        ###############################
####  放量创新低不低吸      ###############





###############################

#########################################

###### R Breaker 卖点 #################

def RBreaker(df_list,SetupNum = 0.02, EnterNum = 0.01):

    trading_list = np.array(df_list[:,0])
    BP_list = np.array(trading_list)

    for i in range(len(df_list)):
        '''
        s_setup = pivot +(high -low)
        s_enter = 2 * pivot -high
        估价先触及s_setup再触及s_enter，卖出
        '''
        try:
            BP_list[i]=int(0)
            df = copy.deepcopy(df_list[i,1])
            if(len(df)>0):
                try:
                    openprice = df['Open']
                    closeprice = df['Close']
                    d_open = openprice[0]
                    s_setup = d_open+SetupNum*d_open   ## 涨幅超过4%
                    s_enter = s_setup - EnterNum*s_setup   ## 回撤超过2%
                    setup_dist = closeprice[closeprice>s_setup].index
                    enter_dist = closeprice[closeprice<s_enter].index
                    if(len(setup_dist)>0 and len(enter_dist)>0):   # 有突破观察价的区间
                        enter_point = enter_dist[enter_dist>setup_dist[0]]
                        if(len(enter_point)>0):
                            enter_point = int(enter_point[0])
                            BP_list[i] = int(enter_point)
                except:
                    print('Get R Breaker Point Error')
                    continue
        except:
            continue
    print('R Breaker BP_list:',BP_list)
    return BP_list
'''
第一步
high = klines.high.iloc[-2] # 前一日的最高价，
low = klines.low.iloc[-2] # 前一日的最低价
close = klines.close.iloc[-2] # 前一日的收盘价
pivot = (high + low + close) / 3 # 枢轴点
第二步
基于上面算出来的四个值，组合出六个不同的价位
b_break = high + 2 * (pivot - low) # 第一组 突破买入价 全场最高
s_setup = pivot + (high - low) # 第二组 观察卖出价 多单叛变条件1
s_enter = 2 * pivot - low # 第二组 反转卖出价 多单叛变条件2
b_enter = 2 * pivot - high # 第三组 反转买入价 空单叛变条件2
b_setup = pivot - (high - low) # 第三组 观察买入价 空单叛变条件1
s_break = low - 2 * (high - pivot) # 第一组 突破卖出价 全场最低
六个价位又分为三组:
第一组、无脑莽：当价格打穿全场最高或者全场最低时，顺势追突破！
第二组、多头叛变：手中持有多单，先触及叛变条件1，后触及叛变条件2后，反手开空。
第三组、空头叛变：手中持有空单，先触及叛变条件1，后触及叛变条件2后，反手开多。
'''
####################################################################



######### 窄幅震荡卖出 ######################################
def LowVolRange(df_list,n=30,LowestVol = 0.01):
    print('LowVolRange Calculating...')
    trading_list = np.array(df_list[:,0])
    BP_list = np.array(np.zeros(len(trading_list)))
    BP_list = BP_list.astype(int)
    for i in range(len(df_list)):
        BP_list[i]=int(0)
        df = copy.deepcopy(df_list[i,1])
        df_delay = pd.DataFrame()
        try:
            closeprice = df['Close']
            delay_price = copy.deepcopy(closeprice)
            for j in range(n):
                delay_price = pd.Series(np.roll(delay_price,1))           ################ 
                delay_price[0]=1.*closeprice[0]
                df_delay = df_delay.append(delay_price,ignore_index = True)
        except:
            continue
        if(len(df_delay)>0):
            HH = df_delay.max(axis=0)
            LL = df_delay.min(axis=0)
            VolRange = HH/LL -1
            BP = VolRange[n:len(VolRange)]
            BP = BP[BP<LowestVol]
            # BP.sort()
            if(len(BP)>0):
                BP = int(BP.index[0])
                BP_list[i] = BP
    # print('LowVolRange BP_list: ',BP_list)
    return BP_list
#####################################################


############ 暴涨延迟 ###################################
def HighVolRange(df_list,BP_list,n=10,HighVolRange = 0.03):
    temBP = copy.deepcopy(BP_list)
    for i in range(len(df_list)):
        if (temBP[i]>0.1):
            try:
                df = copy.deepcopy(df_list[i,1])
                # highprice = df['High']
                lowprice = df['Low']
                closeprice = df['Close']
                ## 循环体，直到涨速降低
                while(temBP[i]<240):
                    cp = closeprice[temBP[i]]  # 卖点收盘价
                    j = int(max(temBP[i]-n,0))  # n分钟前坐标，小于n取0
                    VolRange = cp/lowprice[j]-1   # 与n分钟前的涨跌幅比较
                    if(VolRange > HighVolRange):
                        temBP[i] = temBP[i] +1
                        print('Stock ',i,' delay selling to ',temBP[i])
                    else:
                        break
            except:
                print('HighVol Filter Error.')
                continue
    return temBP
#########################################################

############ 机械点位延迟 ###################################
def MechPointDelay(df_list,BP_list):
    temBP = copy.deepcopy(BP_list)
    for i in range(len(df_list)):
        if (temBP[i]>0.1):
            try:
                df = copy.deepcopy(df_list[i,1])
                closeprice = df['Close']
                AVP = func1.GetAVP(df) ## Get AVP Time Series
                MechPrice = AVP+0.005*AVP
                AboveMech = closeprice[closeprice > MechPrice] # 在机械点位上方
                # print(AboveMech)
                AboveInd = AboveMech.index
                BP = AboveInd[AboveInd>temBP[i]]
                # BP.sort()
                if(len(BP)>0):
                    if(BP[0]>0 and BP[0]<239):
                        temBP[i] = int(BP[0])
                        print('Stock ', i,' Mech Delayed')
                else:  # BP为空集，则没有卖点
                    temBP[i] = int(0)
            except:
                continue
    return temBP
#########################################################


#########################################################
def NosignMechSell_1(df_list,BP_list,n=30):
    # 30分钟后，如果波动率小于2%， 1%机械点卖出
    temBP = copy.deepcopy(BP_list)
    for i in range(len(df_list)):
        if(temBP[i]<0.1 or temBP[i]>n):  # temBP[i]==0 11点没有卖出信号
            try:
                df = copy.deepcopy(df_list[i,1])
                closeprice = df['Close']
                AVP = func1.GetAVP(df) ## Get AVP Time Series
                MechPrice = AVP+0.01*AVP

                DymMech = AVP[0]*1.1 -(AVP.index)*(0.1/n)*AVP[0]
                
                DymMech = DymMech[DymMech>AVP[0]]
                plt.figure(1)
                plt.plot(MechPrice,c='orange')
                plt.plot(DymMech,'blue')
                plt.plot(closeprice,'red')
            except:
                continue
    return temBP
###################################################


######### 无信号机械卖出 #############################################
def NosignMechSell_2(df_list,BP_list,n=30):   # 超过n分钟还没卖出
    temBP = copy.deepcopy(BP_list)
    for i in range(len(df_list)):
        if(temBP[i]<0.1 or temBP[i]>n):  # temBP[i]==0 11点没有卖出信号
            try:
                df = copy.deepcopy(df_list[i,1])
                closeprice = df['Close']
                AVP = func1.GetAVP(df) ## Get AVP Time Series
                MechPrice = AVP+0.005*AVP
                DymMech = MechPrice*1.01-MechPrice*0.01*(MechPrice.index/240.)
                '''
                plt.figure(1)
                plt.plot(MechPrice,c='orange')
                plt.plot(DymMech,'blue')
                plt.plot(closeprice,'yellow')
                '''
                # print(AboveMech)
                AboveMech = closeprice[closeprice > DymMech] # 在机械点位上方
                AboveInd = AboveMech.index
                BP = AboveInd[AboveInd>n]
                BP.sort()
                if(len(BP)>0):
                    if(BP[0]>0 and BP[0]<239):
                        temBP[i] = int(BP[0])
                        print('Stock ',i,' Dynamic Mech Sold.')
            except:
                continue
    return temBP
##################################################################


##########  支撑位买点 ##########################################
def SupportBuyPoint(df_list,n=60):
    print('SupportBuyPoint Calculating...')
    trading_list = np.array(df_list[:,0])
    BP_list = np.array(np.zeros(len(trading_list)))
    BP_list = BP_list.astype(int)
    for i in range(len(df_list)):
        BP_list[i]=int(0)
        df = copy.deepcopy(df_list[i,1])
        df_delay = pd.DataFrame()
        #
        #
        #
        #
    # print('LowVolRange BP_list: ',BP_list)
    return BP_list





#####################################################################


def BackTest(df_list,DayData,index_data,DDate,MinBP=3,SwingCon =1.2,RandTest=False,TestNum=100,BuyOrSell = 'Buy',MaxBuyTimes = 1):
    '''
    connect = pymysql.connect(host =  '192.168.20.200',
                              user = 'root',
                              password = 'ji666888',
                              db = 'minerva_quotation',
                              charset = 'utf8' 
                              )
    cur = connect.cursor()
    print(cur)
    '''
    #sql =  "select * from %s where Time LIKe '%%%s%%' ORDER BY Time" %(stock,DDate)
    #trading_list =  np.array(Stock_List[2000:2300])
    trading_list = np.array(df_list[:,0])
    #DDate = '2022-08-01'
    PnL = 0
    i = 0
    time0 = time.time()
    print("日期：",DDate)
    ##for df in df_list:
    weightData = GetWeight()
    BP_list = np.array(np.zeros(len(trading_list)))
    BP_list = BP_list.astype(int)
    BuyPrice_list = np.array(np.zeros(len(trading_list)))
    BuyTimes = np.array(np.zeros(len(trading_list)))
    # BuyPrice_list = BuyPrice_list.astype(int)
    TotalBuyAmount = 0
    
    ###### 加入R_Breaker策略卖点  ###########
    TrueData = DataReverse(df_list)
    if(BuyOrSell == 'Sell' and RBreakOn == 'True'):
        RB_BP = RBreaker(TrueData,SetupNum = 0.06,EnterNum = 0.02) # 加入R_Breaker策略卖点
        LV_BP = LowVolRange(TrueData,n=60,LowestVol = 0.005)  # 加入LowVolRange策略卖点
        Alt_BP = RB_BP+LV_BP
        #Alt_BP = pd.DataFrame([RB_BP,LV_BP])
        for ii in range(len(RB_BP)):
            if(RB_BP[ii]>0.1 and LV_BP[ii]>0.1):
                Alt_BP[ii] = int(min(RB_BP[ii],LV_BP[ii]))
        
    ### 计算 LowVol 卖点 ################
    if(BuyOrSell == 'Sell' and LowVolOn == True):
        LV_BP = LowVolRange(TrueData,n=30,LowestVol = 0.01)  # 加入LowVolRange策略卖点
    ######################################################

    ############## 计算初步的买卖点点#######################
    output2 = pd.DataFrame()
    TotalBuyPoint = 0

    j = int(-1)
    for stock in trading_list:
        df = []
        j = j + 1
        BP_list[j] = 0
        BuyPrice_list[j] = 0
        BP = []
        
        ########## 读取前一日K数据  ##################
        try:
            DPrice = DayData.loc['d_'+stock[3:9]]          
        except:
            print('Get ExClose Price Failed')
        ####################################################
        try:
            df = df_list[j,1]
        except:
            print("No Stock Data")
            continue
        try:
            #### 原策略 #####################
            if RandTest==False:
                BP = func1.GetBuyLowP(df,DPrice,index_data,
                                      5,MinBP,
                                      SwingCon=SwingCon,
                                      BuyOrSell = BuyOrSell,
                                      MaxBuyTimes=MaxBuyTimes)      ### 震荡策略买卖点
                #print(BP)
                ### 合并 RBreaker 卖点 ################
                if(BuyOrSell == 'Sell' and RBreakOn == True):
                    try:
                        BP = np.append(BP,[int(Alt_BP[j])]) ############## j j-1
                        BP.sort()
                        BP = BP[BP>0]
                        BP = BP[BP<239]
                        BP = BP[0:MaxBuyTimes]
                        #print("  Append RBreaker Sell Point ")
                    except:
                        print("Appending Error")
                ##########################################

                ### 合并 LowVolRange  卖点 ################
                if(BuyOrSell == 'Sell' and LowVolOn == True):          
                    try:
                        BP = np.append(BP,[int(LV_BP[j])])   #！！！！
                        BP.sort()
                        BP = BP[BP>0]
                        BP = BP[BP<239]
                        BP = BP[0:MaxBuyTimes]
                        #print("  Append RBreaker Sell Point ")
                    except:
                        print("Appending Error")
                ##########################################
                
                
                #### 合并高位震荡买点 ######################
                if(BuyOrSell =='Buy'):
                    tt=0
                    #### to be continued...
                #########################################
                
            else:
                ####  测试随机买入 ##############################
                BP = np.random.rand()
                if BP < 0.7:
                    BP = [int(np.random.rand()*238)]
                if (j<10): print('Random Testing...')
                #################################################
                ####################################################
            if len(BP) > 0:
                # print(stock,' ',end = '')
                BP_list[j] = BP[0]  # 只显示第一个买入点，后面的买入点看图
                # BuyPrice_list[j] =df['Open'].iloc[BP[0]+1] # 只显示第一个买价
        except:
            continue
        ###########################################################   
                
        ####### 多买点计算收益率 #####################################
        if(BuyOrSell == 'Buy'):     
            if(len(BP)>0):
                BuyTimes[j] = len(BP)
                for i in range(len(BP)):
                    if(weightOn == True):
                        try:
                            weight = weightData[weightData['代码']== (stock[3:9])].iloc[0,1]
                        except:
                            weight = 10
                    else:
                        weight = 10
                    temBuyPrice = df['Open'].iloc[int(int(BP[i])+1)]
                    BuyPrice_list[j] += temBuyPrice
                    TradeReturn = (df['Close'].iloc[-1] - temBuyPrice)/temBuyPrice
                    TotalBuyPoint +=1
                    TotalBuyAmount += weight
                    PnL += (TradeReturn*weight)
                    #### 保存交易数据
                    output2 = output2.append([[stock,BP[i],temBuyPrice]])

                BuyPrice_list[j] /= len(BP)  #平均买入价格
        ######################################################

    ############## 机械点位后移 ###############################
    # 先关闭机械点位过滤， 再进行机械点位后移
    if(BuyOrSell == 'Sell' and MechDelay == True):
        BP_list = MechPointDelay(TrueData,BP_list)
        #print('Mech Delay Finished.')
    ########################################################
    
    ############# 加入时间机械点位1 #####################
    if(BuyOrSell == 'Sell' and NosignMechOn1 == True):
        BP_list = NosignMechSell_1(TrueData,BP_list,n=60)
       # print('No Signal Mech SellW Finished.')
    #################################################

    
    ############# 加入时间机械点位2 #####################
    if(BuyOrSell == 'Sell' and NosignMechOn2 == True):
        BP_list = NosignMechSell_2(TrueData,BP_list,n=30)
        #print('No Signal Mech Sell Finished.')
    #################################################
    
    ##############  暴涨延迟买卖点 #############################
    if(BuyOrSell == 'Sell' and HighVolDelay == True):
        BP_list = HighVolRange(TrueData,BP_list,n=10,HighVolRange = 0.03)
    ########################################################

    
    print(BP_list)
    print(type(BP_list))
    ############# 高抛计算交易收益率####################
    if(BuyOrSell == 'Sell'):
        j = int(-1)
        for stock in trading_list:
            df = []
            j = j + 1
            #print(BuyPrice_list[j])
            try:
                df = df_list[j,1]
            except:
                print("No Stock Data")
                continue
            ## 根据买入点获取买入价格
            if(BP_list[j]>0 and BP_list[j]<239):
                try:
                    BuyPrice_list[j] = df['Open'].iloc[int(int(BP_list[j])+1)]
                    #print(BuyPrice_list[j])
                except:
                    print('Get BuyPrice Failed')
                    continue
            if (BP_list[j]>0.1):
                try:
                    if(weightOn== True):
                        try:
                            weight = weightData[weightData['代码']== (stock[3:9])].iloc[0,1]
                        except:
                            weight = 10
                    else:
                        weight = 10
                    if(BuyOrSell == 'Sell'):
                        weight = 10
                    BP = BP_list[j]
                    BuyPrice = df['Open'].iloc[BP+1]
                    TradeReturn = (df['Close'].iloc[-1] - BuyPrice)/BuyPrice
                    TotalBuyAmount += weight
                    PnL += (TradeReturn*weight)
                    # print('收益率:',PnL)
                except:
                    print("Trading Failed.")
                    continue
   ##############################################################
    #PnL /= len(trading_list)
    print(DDate,"当日收益率：",round (PnL,4),)
    print("当日交易股票数量为：",len(BP_list[BP_list>0.1]))
    print("当日总买点数量为：",TotalBuyPoint)
    print('总交易金额：',TotalBuyAmount)
    if(TotalBuyAmount>0):
        print('平均收益率：',PnL/TotalBuyAmount*100,'%')
        
    #### 修改卖出的SellPrice ####################################
    if(BuyOrSell == 'Sell'):
        for i in range(len(trading_list)):
            if(BP_list[i]>0.1):
                try:
                    df = TrueData[i,1]
                    BuyPrice_list[i] = df['Open'].iloc[BP_list[i]+1]
                except:
                    print('Buy Stock Failed')
                    continue
    ###########################################################
    if(BuyOrSell=='Sell'):
        output = pd.DataFrame([trading_list,BP_list,BuyPrice_list,BuyTimes]).T
        output.columns = ['股票代码','卖出时间','卖出价格','交易次数']
        print('RunTime = ', time.time()-time0)
        return output
    if(BuyOrSell=='Buy'):
        output2.columns = ['股票代码','买入时间','买入价格',]
        print('RunTime = ', time.time()-time0)
        return output2
    #########################################
    #sql =  "select * from %s where Time LIKe '%%%s%%' ORDER BY Time" %(stock_list,DDate)
    #stock = 'm1_600519'
    #sql = "select * from %s where Time LIKE '%%%s%%' ORDER BY Time" %(stock,DDate)
    #df = pd.read_sql(sql,connect)
    #sql = "select * where table_name = %s" %stock
    
    #df = pd.read_sql(sql,connect)
    ############  如何读取一次数据库，并返回多张表 ##################
    # sql = "select * from m1_600519,m1_000001 where Time LIKE '%2022-07-28%' ORDER BY Time"
    # 笛卡尔乘积..
    # 合并多张表
    #sql = "select column_name from information_schema.columns where table_schema = 'minerva_quotation' and table_name = 'm1_600426'"
    #select * time in columns.names
    '''
    # sql = "select * where column_name in %s" %Stock_List
    l = ' '.join(Stock_List[5:10].tolist())
    #sql = "select * from m1_600519 m1_000001" 
    sql = "select Close from %s" %l
    # sql = "select * from %s" %Stock_List.tolist()
    cur.execute(sql)
    a = cur.fetchall()
    '''
    #cur.close()
    #connect.close()



def SavePic(output,df_list,DayData,index_data,DDate,SwingCon=1.2,BuyOrSell = 'Buy',inver = False,MaxBuyTimes=1):
    
    os.makedirs('F:\hyscode\pic\RB%s%s'%(BuyOrSell,DDate), exist_ok=True)
    print('Saving Pictures...')
    if(BuyOrSell =='Sell'):
        BuyList = output[output['卖出时间']>0]
    if(BuyOrSell == 'Buy'):
        BuyList = output[output['买入时间']>0]
    BuyListName = np.array(BuyList['股票代码'])
    i=0
    for stock in BuyListName:
        i += 1
        #Stock = stock[3:9]
        #df = SQLCode.GetData(Stock,DDate)
        #df = pd.DataFrame(df_list,index=df_list[:,0])
        df_num= np.argwhere(df_list[:,0]==stock)[0,0]
        df = df_list[df_num,1]
        closeprice = np.array(df['Close'])
        cma = func1.MA(closeprice,60)
        AVP = func1.GetAVP(df)
        figureNum = math.ceil(i/9)
        subNum = i-(figureNum*9-9)
        plt.figure(figureNum)
        plt.subplot(3,3,subNum)
        #plt.title('figure %s'%i +'  '+ Stock+' Close '+ DDate[0:10])
        #plt.title(stock[3:9],fontsize=5)
        #  绘制k线和均线
        plt.xticks(fontsize =5)
        plt.yticks(fontsize =5)
        plt.plot(closeprice, color = 'brown', linewidth = 0.5, label=stock[3:9])
        plt.legend(fontsize=3,shadow=False,edgecolor='black',facecolor = 'white')
        plt.plot(cma, color = 'yellow', linewidth = 0.5)
        plt.plot(AVP, color = 'gold', linewidth = 0.5)
        #  绘制买点  (买点可能有多个)
        DPrice = DayData.loc['d_'+stock[3:9]]          
        if(BuyOrSell == 'Buy'):  
            func1.GetBuyLowP(df,DPrice,index_data,SwingCon = SwingCon,BuyOrSell= BuyOrSell,MaxBuyTimes=MaxBuyTimes)
        # 绘制卖点
        if(BuyOrSell == 'Sell'):
            try:
                SP = output[output['股票代码']==stock]
                x = SP.iloc[0,1]+1
                y = SP.iloc[0,2]
                plt.scatter(x,y, s=5, color='blue') 
            except:
                print('Show Sell Piont Failed')
                continue
    
        ######
        if(i%9==0 or  i==len(BuyListName)):
            #print(i)
            plt.savefig('F:\hyscode\pic\RB%s%s\%spic%s.png'%(BuyOrSell,DDate,DDate,figureNum),dpi=500)
            #plt.clf()
            #print('Saved')
            print(' Pictures ',i,'  ',end="")

    print('Saving Finished.')
    return


def combine2Pdf(folderPath, pdfFilePath ):
    files = os.listdir( folderPath )
    pngFiles = []
    sources = []
    for file in files:
        if 'png' in file:
            pngFiles.append( folderPath + file )
    pngFiles.sort()
    outputPDF = Image.open(pngFiles[0])
    pngFiles.pop( 0 )
    for file in pngFiles:
        pngFile = Image.open( file )
        if pngFile.mode == "RGB":
            pngFile = pngFile.convert( "RGB" )
        sources.append( pngFile )
    outputPDF.save( pdfFilePath, "pdf", save_all=True, append_images=sources )
 
'''
folderPath = 'F:/hyscode/pic/2022-08-02/'
pdfFilePath = 'E:/pdf/'
combine2Pdf(folderPath,pdfFilePath)
'''


######  低吸策略运行测试   ################################
'''
if __name__ == '__main__' :
    ########### 获取股票池 ######################################
    stock_list = GetStockList()
    #############################################################
    DDate = '2022-08-04'

    ########### 下载数据 ############################################
    TestNum = 50
    ReadData = True                                        # 下载数据开关
    if(ReadData):
        df_list = GetDataList(stock_list,DDate,TestNum)
        #print(df_list)

    ###############################################################
    
    #########  个股策略测试  #####################################
    Stock = '600426'
    df = SQLCode.GetData(Stock,DDate)
    closeprice = np.array(df['Close'])
    cma = func1.MA(closeprice,60)
    AVP = func1.GetAVP(df)
    plt.figure(1)
    #plt.title(Stock+' Close '+ DDate[0:10])
    plt.plot(closeprice, color = 'brown')
    plt.plot(cma, color = 'yellow')
    plt.plot(AVP, color = 'gold')
    BP = func1.GetBuyLowP(df)
    print(BP)
    #plt.scatter([10,],[28,],s=50,color='g')
    ##############################################################
    # n=60
    # ar_ma = func1.MA(closeprice,n)
    # #print(ar_ma)
    # r = func1.GetReturn(closeprice)A
    # dif = func1.GetDif(closeprice,n)
    #print("是否震荡:",func1.isSwing(closeprice,n))
    #func1.ShowBP(closeprice,BP)

    ############ 回测函数调用 ##################
    output = BackTest(df_list, 
                      DDate,MinBP=5, 
                      RandTest = False)
    print(output)
    output.columns = ['股票代码','买入时间','买入价格']
    output.to_excel('M%s output.xls' %DDate)
    ###########################################
    
    ############ 打印所有买入的股票图片 ###########
    # SavePic(output,df_list,DDate)
    ####################################################
'''










'''

N:=5;
VAR1:4*SMA((CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100,5,1)-
3*SMA(SMA((CLOSE-LLV(LOW,N))/(HHV(HIGH,N)-LLV(LOW,N))*100,5,1),3.2,1),coloryellow,LINETHICK0;
VAR2:8,colorgreen,LINETHICK0;
IF(CROSS(VAR1,VAR2),80,0),STICK,COLOR0000CC,LINETHICK2;
IF(VAR1<=8,25,0),STICK,colorwhite,LINETHICK2;
DRAWTEXT(CROSS(VAR1,VAR2),80,'建仓'),COLOR00FFFF;
VARO5:=LLV(LOW,27);
VARO6:=HHV(HIGH,34);
VARO7:=EMA((CLOSE-VARO5)/(VARO6-VARO5)*4,4)*25;
IF((VARO7<10),80,100) ,COLOR00CCFF,LINETHICK1;
0,LINETHICK2 ,COLORFFCC00;
DRAWTEXT(VAR1<10,VAR1,'始');
DRAWTEXT(VAR1>90,VAR1,'终');
'''
