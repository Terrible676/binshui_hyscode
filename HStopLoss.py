# -*- coding: utf-8 -*-
"""
Created on Tue Aug 30 19:01:03 2022

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
P2On = True

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

def GetAVP(priceData):               # 计算日内均价
    Amount = priceData["Amount"]
    ClosePrice = priceData["Close"]
    Volume = Amount / ClosePrice
    AVP = Amount.cumsum()/Volume.cumsum()
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
    SellList = output[output['卖出时间']>0.1]
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
            x = SellList.loc[stock]['卖出时间']
            y = SellList.loc[stock]['卖出价格']
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
'''
def GEtATR(ExData):
    try:
        High = ExData['High']
        Close = ExData['Close']
        Low = ExData['Low']
        ATR = max(100*(High-Low)/Close,1)
    except:
        ATR =1
    return ATR
'''
##### 放量下跌止损 ##############################
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
        
        ##### 跌幅过大止损卖出  ############
        if(ret_rlt<-4 and ret_cur<-1 and DrawDown > 2 and Speed_Draw>1):
            P1 = True              # 第一类卖点
            return P1
        ######################
        
        ##### 放量下跌止损  ########################
        if(ExVol<100):
            return False
        avp = GetAVP(price)
        dif = (ClosePrice / avp -1) * 100
        cur_ret = (ClosePrice[-1] / ClosePrice[0] -1) * 100
        BelowAVP = dif[dif<0]
        BelowRatio = len(BelowAVP)/len(ClosePrice)
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        # print(TurnOver)
        isPump1 = (avg_Vol*240*(max(-cur_ret,1)) / ExVol > 10)
        
        
        ### 均线以上震荡下跌不止损 ################
        
        
        ##########################################
        
        if((BelowRatio > 0.8) and 
           (len(ClosePrice)>5) and 
           (cur_ret < -1) and 
           (ret_rlt<-1) and
           DrawDown > 2 and
           avg_Amount > 50 and   #平均成交量大于100万
           TurnOver > 1 and 
           isPump1
           ):
            P1 = True
            return P1
        ##########################################
        
        isPump2 = (avg_Vol*240/ExVol > 10)
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

######### 背离止损 ######################################
def Diver2Index(price,tmp_index,Capital):
    P2 = False
    try:
        ClosePrice = np.array(price['Close'])
        #LowPrice =  np.array(price['Low'])
        HighPrice =  np.array(price['High'])
        #OpenPrice =  np.array(price['Open'])
        
        
        ################# 第三版 #########################
        avp = GetAVP(price)
        dif = (ClosePrice / avp -1) * 100
        cur_ret = (ClosePrice[-1] / ClosePrice[0] -1) * 100
        BelowAVP = dif[dif<0]
        BelowRatio = len(BelowAVP)/len(ClosePrice)
        if(BelowRatio < 0.8):
            P2 = False
            return P2
        ################################################



        ret = (ClosePrice[-1]/ClosePrice[0] -1) * 100
        r,p = stats.pearsonr(ClosePrice,tmp_index) # r为相关系数[-1,1]，p为显著程度（概率，p越小概率越高）
        gap = ret - tmp_index[len(tmp_index)-1]
        # 背离大盘c
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        # print('avg_Amount:',avg_Amount)
        #####  最大回撤 DrawDown #########################
        #tmp_index = np.array(tmp_index)
        index_Draw = tmp_index.max() - tmp_index[len(tmp_index)-1]
        DrawDown = (HighPrice.max() / ClosePrice[-1] -1)*100
        ret_DrawDonw = DrawDown - index_Draw
        #Speed_Draw = DrawDown/ (max(len(ClosePrice)/10,1))
        ##################################################
        
        
        if( (r<0.1 or p>0.1) and gap<-1 and ret_DrawDonw>2
           and avg_Amount > 50 and
           TurnOver >1
           ):   # r值小 #或者p值大，代表相关性较弱
            P2 = True
            #print("P2")
            return P2
    except:
        P2 = False
        return P2
    return P2
#######################################################

def isDownTrend(price):
    try:
        ClosePrice = np.array(price['Close'])
        if(len(ClosePrice)<5):
            return True
        else:
            cls_dif = np.diff(ClosePrice)/ClosePrice[0] * 100 # 每分钟涨跌
            greenRatio = len(cls_dif[cls_dif<0])/len(cls_dif)  # 绿k线比例
            maxDown = cls_dif.min()  # 最大下跌速度
            if(maxDown<-0.5 and greenRatio>0.4):
                return True
            else:
                return False
    except:
        return True
    return True

if __name__ == '__main__' :
    
    ########### 获取股票池 ######################################
    # stock_list = Strategy1.GetStockList()
    
    #rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0914.xlsx',dtype=object)
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
        time.sleep(2)

    #### 全股票池 ####################
    slist = Strategy1.GetStockList()
    slist = pd.Series(slist)
    stock_list = slist #rd_j.apply(lambda x:'m1_'+(x[2:8]))
    stock_list2 = slist.apply(lambda x:'d_'+(x[3:9]))

    ### 
    ### 用原始策略回测季总持仓
    ##### 读取市值数据  ########################################################
    read_Capital = pd.read_excel(r'F:/hyscode/CapitalData2.xlsx',dtype=object)
    dm = read_Capital['代码']
    dm = dm.apply(lambda x:x[2:8])
    CapitalData = pd.DataFrame([dm,read_Capital['总市值']]).T
    ########### 下载数据 ############################################
    #BTDate = ['2022-08-19','2022-08-22','2022-08-23','2022-08-24']
    BTDate =['2022-12-22']   # 15-26号
    #BTDate = ['2022-08-29']
    for DDate in BTDate:
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
        SellPoint = np.zeros(len(df_list),dtype = int)
        SellPrice = np.zeros(len(df_list))
        SellType = np.zeros(len(df_list),dtype = object)
        StockName = np.zeros(len(df_list),dtype =object)
        ret_index = GetIndexRet(index_data)       # 获取中证500涨跌数据
        FirstPoint = np.zeros(len(df_list),dtype = int)
        FirstPrice = np.zeros(len(df_list))
        SellTime = np.zeros(len(df_list),dtype = int)
        A10FirstPonit =  np.zeros(len(df_list),dtype = int)
        A10FirstPrice = np.zeros(len(df_list))
        PnL = 0; PnL1 = 0; PnL2= 0
        P1_Point = 0; P2_Point = 0
        BackTest = False  #### 回测或模拟盘
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
                A10First = True
                for j in range(0,120):
                    tmpPrice = PriceData[0:j+1]   #j+1
                    tmp_index = ret_index[0:j+1]
                    # 前三十分钟的止损判断
                    if(BackTest): ## 回测给单点
                        if(j>1 and j<=30 and P1On):
                            P1 = isPumpDown(tmpPrice,ExData,Capital,ret_index[j])
                            if(P1):
                                SellPoint[i] = int(j)
                                SellPrice[i] = PriceData['Open'][j+1]
                                SellType[i] = 'P1'
                                P1_Point += 1
                                PnL1 += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
                                #print(stock,' P1',j)
                                break
                        if(j>10 and j<60 and P2On):
                            P2 = Diver2Index(tmpPrice,tmp_index,Capital)  and isDownTrend(tmpPrice) # 与大盘背离
                            if(P2):
                                SellPoint[i] = int(j)
                                SellPrice[i] = PriceData['Open'][j+1]
                                SellType[i] = 'P2'
                                P2_Point +=1
                                PnL2 += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
                                #print(stock,' P2',j)
                                break
                    else:  ## 实盘实时给点
                        if(j>1 and j<=120 and P1On):
                            P1 = False
                            P1 = isPumpDown(tmpPrice,ExData,Capital,ret_index[j])
                            if(P1):
                                if(SellTime[i] < 1):
                                    FirstPoint[i] = int(j)
                                    FirstPrice[i] = PriceData['Open'][j+1]
                                if(j>29 and A10First): ######### 记录10点后第一个卖点
                                    A10First = False
                                    A10FirstPonit[i] = int(j)
                                    A10FirstPrice[i] = PriceData['Open'][j+1]
                                SellPoint[i] = int(j)
                                SellPrice[i] = PriceData['Open'][j+1]
                                SellType[i] = 'P1'
                                P1_Point += 1
                                PnL1 += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
                                SellTime[i] += 1
                                #print(stock,' P1',j,' ',end='')
                                continue
                        if(j>10 and j<120 and P2On):
                            P2 = False
                            P2 = Diver2Index(tmpPrice,tmp_index,Capital) and isDownTrend(tmpPrice)  # 与大盘背离
                            if(P2):
                                if(SellTime[i] < 1):
                                    FirstPoint[i] = int(j)
                                    FirstPrice[i] = PriceData['Open'][j+1]
                                if(j>29 and A10First):  ######### 记录10点后第一个卖点
                                    A10First = False
                                    A10FirstPonit[i] = int(j)
                                    A10FirstPrice[i] = PriceData['Open'][j+1]
                                SellPoint[i] = int(j)
                                SellPrice[i] = PriceData['Open'][j+1]
                                SellType[i] = 'P2'
                                P2_Point +=1
                                PnL2 += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
                                SellTime[i] += 1
                                #print(stock,' P2',j,' ',end='')
                        
                #if(SellPoint[i]>0):
                #    PnL += (SellPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
            except:
                continue
                
             
        ########### 交易统计 ##################################################################
        #print(SellPoint)
        PnL = PnL1+PnL2
        Total_Sold = len(SellPoint[SellPoint>0])
        PnL /= max(P1_Point+P2_Point,1)
        print(DDate,' Fell止损：')
        print('总股票数量：',len(df_list))
        print('卖出股票数量:',Total_Sold)
        print('平均收益率为：',round(PnL,3),'%')
        print('止损比例为：',round(Total_Sold/len(df_list)*100,3),'%')
        print("P1止损数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')
        print("P2止损数量:",P2_Point,' 收益率:',round(PnL2/max(P2_Point,1),3),'%')

        if(BackTest==True):
            output = pd.DataFrame([SellPoint,SellPrice,SellType]).T#,columns=['卖出时间','卖出价格'])
            output.index = StockName
            output.columns = (['卖出时间','卖出价格','卖出类型'])
        else:
            output = pd.DataFrame([FirstPoint,FirstPrice,A10FirstPonit,A10FirstPrice,SellPoint,SellPrice,SellTime]).T
            output.index = StockName
            output.columns = (['卖出时间','卖出价格','10点后第一卖点','10点后第一卖价','最后卖点','最后卖价','给点次数'])
        BPTime = output['卖出时间']
        BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        prt_output = copy.deepcopy(output)
        prt_stock =  pd.Series(prt_output.index)
        prt_output.index =prt_stock.apply(lambda x:x[3:9])
        prt_output['卖出时间']=BPTime.values
        if(BackTest == False):
            BPTime2 = output['最后卖点']
            BPTime2 = BPTime2.apply(lambda x:Strategy1.GetBPTime(int(x)))
            prt_output['最后卖点']=BPTime2.values
        prt_output = prt_output[prt_output['卖出价格']>0.1]
        prt_output.to_excel('F:/hyscode/FellStop/output/2MStopLoss%s output.xlsx' %DDate)
        ######################################################################################
        
        



