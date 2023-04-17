# -*- coding: utf-8 -*-
"""
Created on Tue Dec  6 17:17:55 2022

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


P1On = True
P2On = False

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

def Save2Pic(df_list,output,DDate):
    os.makedirs(r'F:\hyscode\FellStop\pic\fellStop%s'%(DDate), exist_ok=True)
    print('Saving Pictures...')
    SellList = output[output['买入时间']>0.1]
    # SellListName = np.array(SellList['股票代码'])
    StockName = SellList.index
    i=0
    for stock in StockName:
        i += 1
        df_num= np.argwhere(df_list[:,0]==stock)[0,0]
        df = df_list[df_num,1]
        closeprice = np.array(df['Close'])
        cma = MA(closeprice,60)
        AVP = GetAVP(df)
        figureNum = math.ceil(i/9)
        subNum = i-(figureNum*9-9)
        plt.figure(figureNum,dpi=1000)
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

        ######绘制卖点#####
        try:
            x = SellList.loc[stock]['买入时间']
            y = SellList.loc[stock]['买入价格']
            plt.scatter(x,y, s=5, color='blue') 
        except:
            continue
        
        if(i%9==0 or  i==len(StockName)):
            #print(i)
            plt.savefig(r'F:\hyscode\FellStop\pic\fellStop%s\%spic%s.png'%(DDate,DDate,figureNum),dpi=1000)
            #plt.clf()
            #print('Saved')
            print(' Pictures ',i,'  ',end="")
    print('Saving Finished.')

    return

def GetBP(price,N=5):
    ClosePrice = np.array(price['Close'])
    LowPrice =  np.array(price['Low'])
    HighPrice =  np.array(price['High'])
    OpenPrice =  np.array(price['Open'])
    Var1 = ClosePrice -  LLV(LowPrice,N)
    Var2 = HHV(HighPrice,N) - LLV(LowPrice,N)
    Var3 = Var1/Var2 * 100
    Var3 =  SMA(Var3,5,1)
    Var4 = 4.* Var3 - 3. * MA(Var3, 3)

    Var5 = LLV(LowPrice,30)  # 27
    Var6 = HHV(HighPrice,30)  #34
    Var7 = 100*(ClosePrice -Var5)/(Var6-Var5)
    Var7 = MA(Var7,4)
    Var7 = np.array(Var7)
    
    S1 = Var4
    S2 = S1*0 + 8
    CP = CrossPoint(S1,S2).T
    CP3 = np.argwhere(Var7<10).T  
    BP = np.intersect1d(CP,CP3)
    
    
    return BP
###############################################

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

###### VolFilter ##########

def VolFilter(price,Capital):
    try:
        ClosePrice = price['Close']
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        if(avg_Amount> 30 and TurnOver > 0.5):
            return True
        else:
            return False
    except:
        return False
    return False


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
           (cur_ret < -1) and 
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
        
        isPump2 = (avg_Vol*240/ExVol < 8)
        if(isPump2 and
           (cur_ret < -2) and
           avg_Amount > 100 and
           TurnOver > 1
           ):
            P1 = True
            return P1


        if(BelowRatio > 0.95 and
           (len(ClosePrice)>20) and
           (cur_ret<-2) and 
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


if __name__ == '__main__' :
    
    ########### 获取股票池 ######################################
    # stock_list = Strategy1.GetStockList()
    
    #rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0921.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    #rd_j = rd_excel['代码']
    #stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8]))
    #stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    
    T = True
    StartTime = datetime.time(16, 30, 0, 0)
    while(T):
        print('.',end='')
        NTime = datetime.datetime.now()
        NTime = NTime.time()
        T = NTime < StartTime
        time.sleep(5)

    #### 全股票池 ###################################
    slist = Strategy1.GetStockList()
    slist = pd.Series(slist)
    stock_list = slist #rd_j.apply(lambda x:'m1_'+(x[2:8]))
    stock_list2 = slist.apply(lambda x:'d_'+(x[3:9]))
    ##############################################
    
    
    ### 用原始策略回测季总持仓
    ##### 读取市值数据  ########################################################
    read_Capital = pd.read_excel(r'F:/hyscode/CapitalData2.xlsx',dtype=object)
    dm = read_Capital['代码']
    dm = dm.apply(lambda x:x[2:8])
    CapitalData = pd.DataFrame([dm,read_Capital['总市值']]).T
    ########### 下载数据 ############################################
    #BTDate = ['2022-08-19','2022-08-22','2022-08-23','2022-08-24']
    td = datetime.datetime.now()
    td = td.date()
    DDate = str(td)
    testNum = 5000
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
        df_list = GetDataList(stock_list,DDate,testNum)
        DayData = GetDayData(stock_list2,preDate,testNum)
        index_data = GetIndexData(DDate,preDate)
        #print(df_list)
    
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
                # 前三十分钟的止损判断
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
            #if(BuyPoint[i]>0):
            #    PnL += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
        except:
            continue
       # r= Diver2Index(tmpPrice,tmp_index) 
        #if(r<0.1):
         #   print(stock,' ',round(r,3))
            
    #print(BuyPoint)
    PnL = PnL1+PnL2
    Total_Sold = len(BuyPoint[BuyPoint>0])
    PnL /= max(Total_Sold,1)
    output = pd.DataFrame([BuyPoint,SellPrice,SellType]).T#,columns=['买入时间','买入价格'])
    output.index = StockName
    output.columns = (['买入时间','买入价格','买入类型'])
    
    print(DDate,' 早盘保守低吸2')
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
    os.makedirs(r'F:\hyscode\SweepBuy2\output', exist_ok=True)
    prt_output.to_excel('F:/hyscode/SweepBuy2/output/保守假摔低吸2-%s output.xlsx' %DDate)

                
        #### 计算相对收益率 ###################


