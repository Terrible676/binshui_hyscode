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
import statsmodels.tsa.api as smt


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

def Save2Pic(df_list,output,DDate):
    os.makedirs(r'F:\hyscode\Chase\pic\Chase%s'%(DDate), exist_ok=True)
    print('Saving Pictures...')
    BuyList = output[output['买入时间']>0.1]
    # BuyListName = np.array(BuyList['股票代码'])
    StockName = BuyList.index
    i=0
    for stock in StockName:
        i += 1
        df_num= np.argwhere(df_list[:,0]==stock)[0,0]
        df = df_list[df_num,1]
        closeprice = np.array(df['Close'])
        cma = func1.MA(closeprice,60)
        AVP = func1.GetAVP(df)
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
            x = BuyList.loc[stock]['买入时间']
            y = BuyList.loc[stock]['买入价格']
            plt.scatter(x,y, s=5, color='blue') 
        except:
            continue
        
        if(i%9==0 or  i==len(StockName)):
            #print(i)
            plt.savefig(r'F:\hyscode\Chase\pic\Chase%s\%spic%s.png'%(DDate,DDate,figureNum),dpi=1000)
            #plt.clf()
            #print('Saved')
            print(' Pictures ',i,'  ',end="")

    print('Saving Finished.')

    return

def GEtATR(ExData):
    try:
        High = ExData['High']
        Close = ExData['Close']
        Low = ExData['Low']
        ATR = max(100*(High-Low)/Close,1)
    except:
        ATR =1
    return ATR

#####价格支撑买入##############################
def isSupport(price,ExData,Capital,LastBuyPrice,Ret_index = 0,n=30):
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
        
        ##### 支撑价 ###############
        Supporting = ( ClosePrice[-1]/LowPrice.min() -1) * 100
        Speed_Sup = Supporting/ (max(len(ClosePrice)/10,1))
        ##############################
        ##### TurnOver ################
        
        DrawDown = ( HighPrice.max()/ClosePrice[-1] -1) * 100
        
        #移动最大值：
        #eHH = pd.Series(HighPrice).expanding().max()
        #eLL = pd.Series(LowPrice).expanding().min()
        #epd = (eHH / eLL -1) *100
        #Diff_Num = (ClosePrice / pd.Series(ClosePrice).shift(1) -1)*100
        #Diff_Num[0] = 0
        #acf = smt.stattools.acf(Diff_Num)
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
        Ratio3 = (HH/LL -1) *100
        GoldSpt = (max_index>min_index) and Ratio1<0.5 and Ratio2<0.382 and Ratio3> 1.5
        
        # ##### 强势冲高买入 ############
        # if(ret_rlt> 4 and ret_cur>1 and Supporting > 2 and Speed_Sup>1):
        #     P1 = True             
        #     return P1
        # ######################

        ##### 成交量换手数据  ########################
        avp = func1.GetAVP(price)
        dif = (ClosePrice / avp -1) * 100
        cur_ret = (ClosePrice[-1] / ClosePrice[0] -1) * 100
        AboveAVP = dif[dif>0]
        AboveRatio = len(AboveAVP)/len(ClosePrice)
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        # print(TurnOver)
        # isBoom1 = (avg_Vol*240*(max(-cur_ret,1)) / ExVol > 10)
        if(LastBuyPrice<0):
            isMech = ClosePrice[-1]/avp[len(avp)-1] < 0.995
        else:
            isMech = ClosePrice[-1]/LastBuyPrice < 0.995
        
        #if(GoldSpt ):
        #    P1 = True 
        #    return P1
        if(GoldSpt and isMech):
            P1 = True
            return P1

        if(GoldSpt and AboveRatio > 0.3 and avg_Amount>30 and TurnOver>0.5 and isMech):
            P1 = True
            return P1

        '''
        if((AboveRatio > 0.8) and 
           (len(ClosePrice)>5) and 
           (cur_ret >1) and 
           (ret_rlt >1) and
           Supporting > 2 and
           # Speed_Sup > 1 and
           avg_Amount > 200 and   #平均成交量大于200万
           TurnOver > TurnNum and
           DrawDown < 1 and
           isBoom1
           ):
            P1 = True
            return P1
        ##########################################
        
        # isBoom2 = (avg_Vol*240/ExVol > 10)
        # if(isBoom2 and
        #    (cur_ret > 2) and
        #    avg_Amount > 50 and
        #    TurnOver > TurnNum
        #    ):
        #     P1 = True
        #     return P1
        
        if( AboveRatio > 0.95 and
           len(ClosePrice)>20 and
           cur_ret>2  and 
           DrawDown < 1 and 
           TurnOver > TurnNum
           ):
            P1 = True
            return P1
        '''
    except:
        P1 = False
        return P1
    return P1
##########################################################



######### 逆势上涨 ######################################
def Diver2Index(price,tmp_index,Capital):   # 逆势上涨
    P2 = False
    try:
        ClosePrice = np.array(price['Close'])
        LowPrice =  np.array(price['Low'])
        HighPrice =  np.array(price['High'])
        #OpenPrice =  np.array(price['Open'])
        ret = (ClosePrice[-1]/ClosePrice[0] -1) * 100
        r,p = stats.pearsonr(ClosePrice,tmp_index) # r为相关系数[-1,1]，p为显著程度（概率，p越小概率越高）
        gap = ret - tmp_index[len(tmp_index)-1]
        # 背离大盘
        avg_Vol = price['Vol'].mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量
        TurnOver = avg_Amount*100000000/Capital    # 没点代表一分钟换手率万分之一
        # print('avg_Amount:',avg_Amount)
        #####  支撑位抬升 #########################
        #tmp_index = np.array(tmp_index)
        index_Sup =  tmp_index[len(tmp_index)-1] - tmp_index.min()
        Supporting = ( ClosePrice[-1]/LowPrice.min() /-1)*100
        DrawDown = (HighPrice.max() / ClosePrice[-1] -1) *100
        ret_Sup = Supporting - index_Sup

        #Speed_Draw = DrawDown/ (max(len(ClosePrice)/10,1))
        ##################################################
        if( (r<0.5 or p>0.1) and gap>1  # and ret_Sup > 1
           and DrawDown < 2
           and avg_Amount > 50  
           and TurnOver>TurnNum
           ):   # r值小或者p值大，代表相关性较弱
            P2 = True
            #print("P2")
            return P2
    except:
        P2 = False
        return P2
    return P2
#######################################################

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
    
    ########### 获取股票池 ######################################
    # stock_list = Strategy1.GetStockList()
    
    rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0908.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    rd_j = rd_excel['代码']
    stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8]))
    stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    ### 用原始策略回测季总持仓
    ##### 读取市值数据  ########################################################
    read_Capital = pd.read_excel(r'F:/hyscode/CapitalData2.xlsx',dtype=object)
    dm = read_Capital['代码']
    dm = dm.apply(lambda x:x[2:8])
    CapitalData = pd.DataFrame([dm,read_Capital['总市值']]).T
    ########### 下载数据 ############################################
    #BTDate = ['2022-08-19','2022-08-22','2022-08-23','2022-08-24']
    BTDate =['2022-09-07']  
    for DDate in BTDate:
        testNum = 1000
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
        StockName = np.zeros(len(df_list),dtype =object)
        ret_index = GetIndexRet(index_data)       # 获取中证500涨跌数据
        output = pd.DataFrame()
        PnL = 0; PnL1 = 0; PnL2= 0
        P1_Point = 0; P2_Point = 0
        for i in range(len(df_list)):
            LastBuyPrice = -1
            BuyTimes = 0
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
                for j in range(1,len(PriceData)):
                    if(BuyTimes >= MaxBuyTime):
                        break
                    tmpPrice = PriceData[0:j]
                    tmp_index = ret_index[0:j]
                    if(j>29 and j<234 and P1On):
                        P1 = isSupport(tmpPrice,ExData,Capital,LastBuyPrice,ret_index[j],)
                        if(P1):
                            BuyPrice[i] = PriceData['Open'][j+1]
                            if(BuyPrice[i]>0.1):
                                BuyPoint[i] = int(j)
                                BuyType[i] = 'P1'
                                LastBuyPrice = BuyPrice[i]
                                P1_Point += 1
                                PnL1 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                                BuyTimes += 1
                                output = output.append([[stock, BuyPoint[i], BuyPrice[i]]])
                                print(stock,' P1',j)
                    if(j>180 and j<234 and P2On):
                        P2 = Diver2Index(tmpPrice,tmp_index,Capital)  # 与大盘背离
                        if(P2):
                            BuyPrice[i] = PriceData['Open'][j+1]
                            if(BuyPrice[i]>0.1):
                                BuyPoint[i] = int(j)
                                BuyType[i] = 'P2'
                                LastBuyPrice = BuyPrice[i]
                                P2_Point +=1
                                PnL2 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                                print(stock,' P2',j)
                #if(BuyPoint[i]>0):
                #    PnL += (BuyPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
            except:
                continue
           # r= Diver2Index(tmpPrice,tmp_index) 
            #if(r<0.1):
             #   print(stock,' ',round(r,3))
                
        #print(BuyPoint)
        PnL = PnL1+PnL2
        Total_Buy = len(BuyPoint[BuyPoint>0])
        PnL /= max(Total_Buy,1)
        output = pd.DataFrame([BuyPoint,BuyPrice,BuyType]).T#,columns=['卖出时间','卖出价格'])
        output.index = StockName
        output.columns = (['买入时间','买入价格','买入类型'])
        
        ##### 下一交易日收盘收益率  #################################
        #output = output[output['买入时间']>0.1]
        #NextRet = GetNextDayRet(stock_list2,output,DDate,testNum)
        
        
        
        #############################################################
        output = output[output['买入时间']>0.1]
        print(DDate,' 低吸策略:')
        print('总股票数量：',len(df_list))
        print('买入股票数量:',len(output.index.unique()))
        print('买入点数:',Total_Buy)
        print('买入比例为：',round(Total_Buy/len(df_list)*100,3),'%')
        print("P1买入数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')
        print("P2买入数量:",P2_Point,' 收益率:',round(PnL2/max(P2_Point,1),3),'%')
        print('当日收益率：',round(PnL,3),'%')
        #print("次日收益率：",round(NextRet.mean()-PnL,3),'%')
        

        BPTime = output['买入时间']
        BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        prt_output = copy.deepcopy(output)
        prt_stock =  pd.Series(prt_output.index)
        prt_output.index =prt_stock.apply(lambda x:x[3:9])
        prt_output['买入时间']=BPTime.values
        prt_output = prt_output[prt_output['买入价格']>0.1]
        os.makedirs(r'F:\hyscode\NewBuyLow\output', exist_ok=True)
        prt_output.to_excel('F:/hyscode/NewBuyLow/output/Chase%s output.xlsx' %DDate)

