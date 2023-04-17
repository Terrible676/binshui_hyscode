# -*- coding: utf-8 -*-
"""
Created on Mon Dec 19 14:56:34 2022

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


def MA(price,n):
    #print(MechPoint)
    if len(price)<n:
        return price
    else:
        price1 = 1.*price
        p=0.*price
        for i in range(0,n):
            p = p + price1
            price1 = np.roll(price1,1)
            price1[0]=1.*price[0]
        p /= n
        return p
def LLV(price,n):
    ## a.min(0)每列最小值 a.min(1)每行最小值
    LL = pd.Series(price)
    d = pd.DataFrame()
    for i in range(0,n):
        d = pd.concat([d,LL],axis=1) 
        LL = np.roll(LL,1)
        LL = pd.Series(LL)
        LL[0] = 1.*LL[1]    ##   修复LL[0] = 1.*LL[1] 的bug
        #print(LL)
    return d.min(1)

def HHV(price,n):
    HH = pd.Series(price)
    d = pd.DataFrame()
    for i in range(0,n):
        d = pd.concat([d,HH],axis=1) 
        HH = np.roll(HH,1)
        HH = pd.Series(HH)
        HH[0] = 1.*HH[1]    ##   修复LL[0] = 1.*HH[1] 的bug
        #print(LL)
    return d.max(1)
    
def CrossPoint(S1,S2):
    #plt.plot(S1)
    #plt.plot(S2)
    S = np.array(S1 - S2)
    SS = np.roll(S,1)
    SS[0] = S[0]
    multi = np.array(S * SS)
    CP1 = np.argwhere(multi<-0.).T
    CP2 = np.argwhere(S<0).T
    #for i in CP: 
    #    if S1[i[0]]>S2[i[0]]:
    CP = np.intersect1d(CP1,CP2)
    return CP

def SMA(price,n,M):
    if len(price)<n:
        return price
    else:
        price1 = 1.*price
        p=0.*price
        for i in range(0,n):
            p = ((n-M)*p + M*price1)/n
            price1 = np.roll(price1,1)
            price1[0]=1.*price[0]
        return p    


def GetAVP(priceData):               # 计算日内均价
    Amount = priceData["Amount"]
    #ClosePrice = priceData["Close"]
    Volume = priceData['Vol']
    AVP = 0.01 * Amount.cumsum()/Volume.cumsum()
    return AVP


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

def GetStockList(): #读取中证1000成分股
    rd_excel = pd.read_excel(r'000852closeweight.xlsx',dtype=object)
    StockList = rd_excel['成分券代码']
    return StockList

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


##### Reverse #################################
def isMech(price,ExData,Capital,Ret_index = 0,n=30):
    try:
        ClosePrice = np.array(price['Close'])
        ##### isReverse  ########################
        avp = GetAVP(price)
        dif = (ClosePrice / avp -1) * 100
        
        cur_ret = (ClosePrice[-1] / ClosePrice[0] -1) * 100
        
        if(dif[len(dif)-1]<-0.5 and cur_ret<0):
            return True
        else:
            return False
        # print(TurnOver)
        #############################################
    except:
        return False
    return False
##########################################################

########################################################
def isPunch(price,ExData):   # 冲高回落去除
    try:
        HighPrice = np.array(price['High'])
        ExClose = ExData['ExClose']
        punch = (HighPrice.max() /ExClose -1)*100
        if( punch>2 ):
            return True
        else:
            return False
    except:
        return False
    return False
##########################################################



##### 放量下跌低吸 ##############################
def isPumpDown(price,ExData,Capital,Ret_index = 0,n=30):
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

        ret_cur =  (ClosePrice[-1]/OpenPrice[0] -1) * 100
        
        ##### DrawDown ###############
        DrawDown = (HighPrice.max() / ClosePrice[-1] -1) * 100
        Speed_Draw = DrawDown/ (max(len(ClosePrice)/10,1))
        ##############################
        ##### TurnOver ################
        
        
        ############################
        
        ##### 跌幅过大止损买入  ############
        if(ret_rlt>-5 and ret_cur<-1 and DrawDown > 2 and Speed_Draw>1):
            P1 = True              # 第一类卖点
            return P1
        ######################
        
        ##### 放量下跌止损  ########################
        avp = GetAVP(price)
        dif = (ClosePrice / avp -1) * 100
        cur_ret = (ClosePrice[-1] / ClosePrice[0] -1) * 100
        BelowAVP = dif[dif<0]
        BelowRatio = len(BelowAVP)/len(ClosePrice)
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        # print(TurnOver)
        if(ExVol < 1):
            return False
        isPump1 = (avg_Vol*240*(max(-cur_ret,1)) / ExVol < 10)
        
        
        ### 均线以上震荡下跌不止损 ################
        
        
        ##########################################
        
        if((BelowRatio > 0.7) and 
           (len(ClosePrice)>5) and 
           (cur_ret < -2.5) and 
           (ret_rlt<-1) and
           DrawDown > 2 and
           Speed_Draw > 1 and
           avg_Amount < 1000 and   #平均成交量大于100万
           TurnOver > 0.5 and
           TurnOver < 2 and 
           isPump1
           ):
            P1 = True
            return P1
        ##########################################
        
        isPump2 = (avg_Vol*240/ExVol < 10)
        if(isPump2 and
           (cur_ret < -2.5) and
           avg_Amount > 100 and
           TurnOver > 1
           ):
            P1 = True
            return P1


        if(BelowRatio > 0.95 and
           (len(ClosePrice)>20) and
           (cur_ret<-2.5) and 
           TurnOver > 1
           ):
            P1 = True
            return P1

    except:
        P1 = False
        return P1
    return P1
##########################################################


def isUpDown(price,ExData):   # 冲高回落
    try:
        ExClose = ExData['Close']
        HighPrice = price['High']
        HH = HighPrice.max()
        mRe = (HH/ExClose -1)*100
        if(mRe):
            return False
        else:
            return True
    except:
        return True
    
    return True


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

def ReshapeData(StockDayData,InxDayData):
    print('Reshaping Data...')
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
    print('Data Reshaped.')
    return  CloseMat,OpenMat,LowMat,HighMat,AmountMat

def ReshapeMinData(minData):
    print('Reshaping Data...')
    stock_list = minData[:,0]
    stock_list = pd.Series(stock_list).apply(lambda x:x[3:9])
    #data_list = InxDayData['Time']
    #data_list = np.array(data_list.apply(lambda x:str(x)[0:10]))
    data_list = np.array(range(0,240))
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
            tmp = minData[i,1]
            closeprice = tmp['Close']
            highprice = tmp['High']
            lowprice = tmp['Low']
            openprice = tmp['Open']
            amount = tmp['Amount']
            # stockdate = tmp['Time']
            # stockdate = np.array(stockdate.apply(lambda x:str(x)[11:]))
            closedata = pd.DataFrame(np.array(closeprice),index = data_list)
            highdata =  pd.DataFrame(np.array(highprice),index = data_list)
            lowdata = pd.DataFrame(np.array(lowprice),index = data_list)
            opendata = pd.DataFrame(np.array(openprice),index = data_list)
            amountdata = pd.DataFrame(np.array(amount),index = data_list)
        except:
            closedata = pd.DataFrame(np.zeros(len(data_list)),index = data_list)
            highdata =  pd.DataFrame(np.zeros(len(data_list)),index = data_list)
            lowdata = pd.DataFrame(np.zeros(len(data_list)),index = data_list)
            opendata = pd.DataFrame(np.zeros(len(data_list)),index = data_list)
            amountdata = pd.DataFrame(np.zeros(len(data_list)),index = data_list)
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

    print('Data Reshaped.')
    return  CloseMat,OpenMat,LowMat,HighMat,AmountMat


'''
def GetThisDayData(DDate,StockDayData):
    ThisDayData=[]
    stock_list = StockDayData[:,0] # 股票代码
    for i in range(len(stock_list)):
        pass
    return ThisDayData
'''

def BookSize(alpha,ClosePrice):
    totalcapital = 100000000
    #vol = alpha.apply(lambda x:(x>0)*x)
    vol = (alpha>0)*alpha
    vol = vol/(vol.sum()+1)
    vol = vol.fillna(0)
    vol = vol.apply(lambda x:min(x,0.01)) # 个股最大持仓1%
    vol *= totalcapital
    vol = (vol/(ClosePrice*100))
    vol = vol.fillna(0)
    vol = vol.apply(lambda x:int(x))
    return vol



def NDaysRet(N,StockDayData):
    stock_list = StockDayData[:,0] # 股票代码
    for i in range(len(stock_list)):
        stock = stock_list[i]
        tmp = StockDayData[i,1]
        closeprice = tmp['Close']
        DDate = tmp['Time']
    return


def GetDelayData(CloseMat,TradingDate,i,N):
    DelayDate = TradingDate[int(max(i-N,0))]
    DelayData = CloseMat.loc[DelayDate]
    return DelayData

def GetNDayData(CloseMat,TradingDate,i,N):
    #FirstDate = TradingDate[int(max(i-1-N,0))]
    #EndDate = TradingDate[i]
    PeriodN = TradingDate[int(max(i+1-N,0)):i+1]
    NDayData = CloseMat.loc[PeriodN]
    return NDayData


def GetRltPnL(PnL,InxDayData):
    InxDate =  InxDayData['Time']
    InxDate = InxDate.apply(lambda x:str(x)[0:10])
    InxRe = pd.Series(InxDayData['Close'].values,index = InxDate)
    TradingDate = PnL.index
    InxRe = InxRe[TradingDate]
    InxRe = InxRe / InxRe[0] -1 
    RltPnL = (PnL/100000000 - InxRe) *100
    return RltPnL,(InxRe*100)

def delayAlpha(alphaMat,alpha,n=4):
    alphaTmp = alphaMat.iloc[-n:]
    deAlpha = alphaTmp.mean()
    deAlpha = (deAlpha + alpha)/2
    return deAlpha

def ReStatusMat(StatusMat,Status15Mat):  ### 把第一个1变2改成1.5，由3变2改为1.5，并用Status15Mat过滤
    StatusMat0 = StatusMat.shift(1)  ## 昨日状态矩阵
    StatusMat0 = StatusMat0.fillna(0)
    StatusMat0 = StatusMat0.astype(int) # 转换成整数
    Mat1 = (StatusMat0==1)
    Mat2 = (StatusMat0==2)
    MatTo15 = (StatusMat==2) & Mat1
    MatTo25 = (StatusMat==3) & Mat2
    ReMat = StatusMat-0.5*MatTo15*Status15Mat - 0.5*MatTo25 
    ReMat = ReMat -0.5*(StatusMat0==3)*(StatusMat==2)*Status15Mat
    return ReMat

def Fill0Status(StatusMat):  # 把0变成昨日状态
    StatusMat0 = StatusMat.shift(1)  ## 昨日状态矩阵
    StatusMat0 = StatusMat0.fillna(0)
    StatusMat0 = StatusMat0.astype(int) # 转换成整数
    Mat0 = (StatusMat == 0)
    ReMat = StatusMat + Mat0*StatusMat0
    return ReMat

def is24(tmpPrice,DayHigh,DayLow):
    try:
        lastprice = np.array(tmpPrice['Close'])[-1]
        HighRe = lastprice/DayHigh
        LowRe = lastprice/DayLow
        HighToLow = DayHigh/DayLow
        if((HighRe<0.93 or LowRe<1.02) and HighToLow>1.1):
            return True
        else:
            return False
    except:
        return False
    return False



if __name__ == '__main__' :

    AllTradingDate = GetTradingDate()
    td = datetime.datetime.now()
    td = td.date()

    StartDate = '2022-08-01'
    #EndDate = '2023-01-18'
    EndDate = str(td)
    TradingDate = AllTradingDate[AllTradingDate>=StartDate]
    TradingDate = TradingDate[TradingDate<=EndDate]
    AllStockList = pd.Series(GetAllStockList())
    InxDayData = GetInxDayData()
    testNum = 6000  # 测试股票数量
    StockList = AllStockList[:testNum]
    StockList = StockList.apply(lambda x:x[3:9])
    
    # StockList = pd.Series(['000786'])
    StockDayData = GetStockDayData(StockList)
    
    CloseMat,OpenMat,LowMat,HighMat,AmountMat = ReshapeData(StockDayData,InxDayData)
        
    ##########  BackTest 计算前一交易日前所有交易日的1234 #############
    MinMat = pd.DataFrame(index = StockList)
    MaxMat = pd.DataFrame(index = StockList)
    
    
    
    Status1Mat = pd.DataFrame(index = StockList)
    Status2Mat = pd.DataFrame(index = StockList)
    Status3Mat = pd.DataFrame(index = StockList)
    Status4Mat = pd.DataFrame(index = StockList)
    Status15Mat = pd.DataFrame(index = StockList)
    PnL = pd.Series(np.zeros(len(TradingDate)),index = TradingDate)
    turnover = 0
    i = -1
    N = 60
    for DDate in TradingDate[0:-1]:
        i += 1
        lastDate = TradingDate[i-1]
        ClosePrice = CloseMat.loc[DDate]
        HighPrice = HighMat.loc[DDate]
        LowPrice = LowMat.loc[DDate]
        OpenPrice = OpenMat.loc[DDate]
        
        #CloseMat.cummin()
        NDayData = GetNDayData(CloseMat,TradingDate,i,N)
        MinData = NDayData.min()
        MinData.name = TradingDate[i]
        MinMat = MinMat.join(MinData)
        MaxData = NDayData.max()
        MaxData.name = TradingDate[i]
        MaxMat = MaxMat.join(MaxData)
        ReToLow = (ClosePrice / MinData -1) * 100
        ReToHigh = (MaxData / ClosePrice -1) * 100
        
        ###  Status 1 #######################
        Con0 = ReToHigh < 5
        Con1 = ReToLow>5
        # Con2 = ReToLow<15
        Status1 = (Con0 & Con1 )*1
        Status1Mat = Status1Mat.join(Status1)
        #####################################
        
        ###  Status 15 #####################
        OpenRe = (ClosePrice/OpenPrice -1)*100
        Con9 = OpenRe<-1
        # Con10 = ReToHigh>5
        Status15 =  Con9 * 1
        Status15Mat = Status15Mat.join(Status15)
        ###################################
    
        ### Status 2 ########################
        Con3 = ReToHigh > 5
        Con4 = ReToHigh < 20
        Status2 = (Con3 & Con4)*1
        Status2Mat = Status2Mat.join(Status2)
        ###################################
        
        ### Status 3 #####################
        Con5 = ReToHigh >20
        Con6 = ReToLow >5
        Status3 = (Con5 & Con6)*1
        Status3Mat = Status3Mat.join(Status3)
        ################################
        
        ### Status 4 ########################
        Con7 = ReToHigh>20
        Con8 = ReToLow<5
        Status4 = (Con7 & Con8)*1
        Status4Mat = Status4Mat.join(Status4)
        ###################################
        
    # alphaMat = alphaMat.T
    MinMat = MinMat.T
    MaxMat = MaxMat.T
    
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(10)

    ### Last Day Data ###################
    LastDate = EndDate
    DDate = EndDate
    preDate = TradingDate[-2]
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

    LastMin = MinMat.iloc[-1]
    LastMax = MaxMat.iloc[-1]
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
            DayHigh = LastMax[i]
            DayLow = LastMin[i]
            try:
                Num = np.argwhere(np.array(CapitalData['代码'])==stock[3:9])[0][0]
                Capital = CapitalData.iloc[Num][1]
            except:
                Capital = 10000000000 # 没有数据则以100亿计算
            # BP = GetBP(PriceData)
            #if(len(BP)>1):  # 删掉前两个点
            #    BP=BP[1:]
            ## 低价股过滤 ####
            if(ExData['ExClose']<4.5):
                print('低价股',end='')
                continue
            for j in range(4,30):
                tmpPrice = PriceData[0:j+1]   #j+1
                tmp_index = ret_index[0:j+1]
                
                ## 2\4不买 #####
                Con24 = is24(tmpPrice,DayHigh,DayLow)
                if Con24:
                    break
                Pch = False
                Pch = isPunch(tmpPrice,ExData)  ## 冲高回落不买
                if Pch:
                    break
                P1 = isPumpDown(tmpPrice,ExData,Capital,ret_index[j]) and isMech(tmpPrice,ExData,Capital,ret_index[j])
                #P1 = isMech(tmpPrice,ExData,Capital,ret_index[j]) 
                if(P1):
                    # j += 5 # 延迟5分钟买入
                    BuyPoint[i] = int(j)
                    SellPrice[i] = PriceData['Open'][j+1]
                    SellType[i] = 'P1'
                    P1_Point += 1
                    PnL1 += ((np.array(PriceData['Close'])[-1])/SellPrice[i] -1)*100
                    print(stock,' P1',j,' ',end='')
                    break
                '''
                if(len(BP)>1 and (j in BP)):
                    P2 = isMech(tmpPrice,ExData,Capital,ret_index[j]) and VolFilter(tmpPrice,Capital)
                    #P1 = True
                    if(P2):
                        # j += 5  # 5分钟后买入
                        BuyPoint[i] = int(j)
                        SellPrice[i] = PriceData['Open'][j+1]
                        SellType[i] = 'P2'
                        P2_Point += 1
                        PnL2 += ((np.array(PriceData['Close'])[-1])/SellPrice[i] -1)*100
                        print(stock,' P2',j,' ',end='')
                        break
                '''  
            #   if(BuyPoint[i]>0):
            #   PnL += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
        except:
            continue
       # r= Diver2Index(tmpPrice,tmp_index) 
       # if(r<0.1):
       # print(stock,' ',round(r,3))
            
    #print(BuyPoint)
    PnL = PnL1+PnL2
    Total_Sold = len(BuyPoint[BuyPoint>0])
    PnL /= max(Total_Sold,1)
    output = pd.DataFrame([BuyPoint,SellPrice,SellType]).T#,columns=['买入时间','买入价格'])
    output.index = StockName
    output.columns = (['买入时间','买入价格','买入类型'])
    
    print(DDate,' 早盘保守低吸3_1234')
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
    os.makedirs(r'F:\hyscode\SweepBuy3_1234\output', exist_ok=True)
    prt_output.to_excel('F:/hyscode/SweepBuy3_1234/output/保守假摔低吸3-%s output.xlsx' %DDate)

    # mClose,mOpen,mLow,mHigh,mAmount = ReshapeMinData(minData)  ### 分钟数据
    
    
    #####################################

    '''
    Status1Mat = Status1Mat.T * 1
    Status2Mat = Status2Mat.T * 2
    Status3Mat = Status3Mat.T * 3
    Status4Mat = Status4Mat.T * 4
    Status15Mat = Status15Mat.T
    # StatusMat = Status1Mat*1 + Status2Mat*2 + Status3Mat*3 + Status4Mat* 4
    StatusMat = np.maximum(Status1Mat,Status2Mat)
    StatusMat = np.maximum(StatusMat,Status3Mat)
    StatusMat = np.maximum(StatusMat,Status4Mat)
    
    ### 把第一个2变成1.5，把第一个3变成2.5 ################
    DayMat = ReStatusMat(StatusMat,Status15Mat)
    DayMat = Fill0Status(DayMat)
    print(DayMat)
    DayMat = DayMat.T
    # DayMat.to_excel(r'output\1234output\DayMat%s_%s.xlsx'%(EndDate,j))
#############################################################

############## 计算当日每分钟1234 ###########################
    DDate = EndDate
    MinMat = pd.DataFrame(index = StockList)
    MaxMat = pd.DataFrame(index = StockList)
    Status1Mat = pd.DataFrame(index = StockList)
    Status2Mat = pd.DataFrame(index = StockList)
    Status3Mat = pd.DataFrame(index = StockList)
    Status4Mat = pd.DataFrame(index = StockList)
    Status15Mat = pd.DataFrame(index = StockList)

    for i in range(2,30):
        ##### 拟合当前分钟日k ###############
        virClose = mClose.iloc[i]
        virOpen = mOpen.iloc[0]
        virHigh = mHigh.iloc[0:i+1].max()
        virLow = mLow.iloc[0:i+1].min()
        virAmount = mAmount.iloc[0:i+1].sum()
        CloseMat.iloc[-1] = virClose
        HighMat.iloc[-1] = virHigh
        OpenMat.iloc[-1] = virOpen
        LowMat.iloc[-1] = virLow
        AmountMat.iloc[-1] = virAmount
        ####################################
        
        ## 计算当前1234
        ClosePrice = CloseMat.loc[DDate]
        HighPrice = HighMat.loc[DDate]
        LowPrice = LowMat.loc[DDate]
        OpenPrice = OpenMat.loc[DDate]
        ClosePrice.name = i
        HighPrice.name = i
        LowPrice.name = i
        OpenPrice.name = i
        NDayData = GetNDayData(CloseMat,TradingDate,len(TradingDate),N)
        MinData = NDayData.min()
        MinData.name = i
        MinMat = MinMat.join(MinData)
        MaxData = NDayData.max()
        MaxData.name = i
        MaxMat = MaxMat.join(MaxData)
        ReToLow = ((ClosePrice+0.01) / (MinData+0.01) -1) * 100
        ReToHigh = ((MaxData+0.01) / (ClosePrice+0.01) -1) * 100
        
        ###  Status 1 #######################
        Con0 = ReToHigh < 5
        Con1 = ReToLow>5
        # Con2 = ReToLow<15
        Status1 = (Con0 & Con1 )*1
        Status1Mat = Status1Mat.join(Status1)
        #####################################
        
        ###  Status 15 #####################
        OpenRe = ((ClosePrice+0.01)/(OpenPrice+0.01) -1)*100
        Con9 = OpenRe<-1
        # Con10 = ReToHigh>5
        Status15 =  Con9 * 1
        Status15Mat = Status15Mat.join(Status15)
        ###################################
    
        ### Status 2 ########################
        Con3 = ReToHigh > 5
        Con4 = ReToHigh < 20
        Status2 = (Con3 & Con4)*1
        Status2Mat = Status2Mat.join(Status2)
        ###################################
        
        ### Status 3 #####################
        Con5 = ReToHigh >20
        Con6 = ReToLow >5
        Status3 = (Con5 & Con6)*1
        Status3Mat = Status3Mat.join(Status3)
        ################################
        
        ### Status 4 ########################
        Con7 = ReToHigh>20
        Con8 = ReToLow< 5
        Status4 = (Con7 & Con8)*1
        Status4Mat = Status4Mat.join(Status4)
        ##################################### 
        
        BuyStatus = Status1+Status3
        tmpList = BuyStatus[BuyStatus>0]
        tmpList = tmpList.index
        for stock in tmpList:
            
        

    # alphaMat = alphaMat.T
    MinMat = MinMat.T
    MaxMat = MaxMat.T
    Status1Mat = Status1Mat.T * 1
    Status2Mat = Status2Mat.T * 2
    Status3Mat = Status3Mat.T * 3
    Status4Mat = Status4Mat.T * 4
    Status15Mat = Status15Mat.T
    # StatusMat = Status1Mat*1 + Status2Mat*2 + Status3Mat*3 + Status4Mat* 4
    StatusMat = np.maximum(Status1Mat,Status2Mat)
    StatusMat = np.maximum(StatusMat,Status3Mat)
    StatusMat = np.maximum(StatusMat,Status4Mat)
    T0Mat = copy.deepcopy(StatusMat)
    for i in range(len(T0Mat)):
        lastSta = (DayMat.T).iloc[-1]
        T0Mat.iloc[i] = StatusMat.iloc[i] - 0.5 * (StatusMat.iloc[i]==2) *(lastSta == 1) * Status15Mat.iloc[i] # 由1变2标记为1.5
        T0Mat.iloc[i] = T0Mat.iloc[i] -0.5*(T0Mat.iloc[i]==2)*(T0Mat.iloc[i]==3)*Status15Mat.iloc[i]     # 由3变2标记为1.5
        T0Mat.iloc[i] = T0Mat.iloc[i] + (T0Mat.iloc[i]==0)*lastSta                   # 状态0转换成前一日状态
    print(T0Mat)
    T0Mat = T0Mat.T
    # T0Mat.to_excel(r'output\1234output\T0Mat%s_%s.xlsx'%(EndDate,j))
    #############################################################################
    '''
    
    '''
    ######### 策略统计数据 ###############################
    pltName = datetime.datetime.now().time()
    pltName = str(pltName)[0:8]
    # plt.title(alphaText+pltName)    
    # plt.savefig('output\spic.png',dpi=500)
    RltPnL,InxRe= GetRltPnL(PnL,InxDayData)
    plt.title('1234'+alphaText)
    plt.plot(PnL.values/1000000,label = 'TotalReturn')
    plt.plot(RltPnL.values,label = 'ExcReturn')
    plt.plot(InxRe.values,label = 'ZZ500')
    plt.legend()
    #plt.savefig('output\1234RltPnL.png',dpi=500) 
    
    turnover /= 100000000
    turnover /= len(TradingDate)
    # output = pd.DataFrame(PnL,index = TradingDate)
    
    print(PnL)
    print('日均换手率：',turnover*100,'%')
    # StatusMat.to_excel(r'output\StatusMat.xlsx')

    ## 把文本转换成代码运行#####
    #exec('print(PnL)')
    #text = '1/(ClosePrice * ClosePrice)'
    #alpha = eval(text)
    ############################################################
    
    '''
    '''
    '''
