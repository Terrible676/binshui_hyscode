# -*- coding: utf-8 -*-
"""
Created on Tue Jul 26 11:09:38 2022

@author: MC
"""

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd



MechPoint = 0.5
SwingN = 60
rang =0.10
MechOn = True           # 机械点   if Sell MechOn = False,Buy 为 True        
SwingOn = False          # 震荡过滤 if Sell SwingOn = False， Buy 为 True    
BelowAVPOn = True       # 均价过滤  if Sell SwingOn = False， Buy 为 True   
DownOn =  True        # 跌幅过滤       
## 第五版False #######################
MultiDelayOn = False     # 机械点位延迟交易
LinearBuyOn = False      # 线性买点
DownTrendDelay = False    # 5分钟k线下跌延迟
SupportDelay = False      # 支撑位延迟交易
IndexOn = False           # 考虑指数的参数调整
LowVolBuyOn = False      # 横盘买入
#############################################

####### 第六版 True #####################
#IndexOn = False           # 考虑指数的参数调整
#LowVolBuyOn = False      # 横盘买入
########################################

MultiOn = True
VolDown = True          # 放量下跌
# MaxBuyTimes = 1

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
    ClosePrice = priceData["Close"]
    Volume = Amount / ClosePrice
    AVP = Amount.cumsum()/Volume.cumsum()
    return AVP
    

'''
def SMA(S, N, M=1):   
    K = pd.Series(S).rolling(N).mean()    #先求出平均值
    for i in range(N+1, len(S)):  K[i] = (M * S[i] + (N -M) * K[i-1]) / N  # 因为要取K[i-1]，所以 range(N+1, len(S))        
    return K
'''
def GetReturn(price):
    price1 = np.roll(price,1)
    price1[0] = price[0]
    r = price1/price - 1
    return r

def GetOpenPrice(price):
    OpenPrice = np.array(price['Open'])
    return OpenPrice

def GetClosePrice(price):
    ClosePrice = np.array(price['Close'])
    return ClosePrice

def GetHighPrice(price):
    HighPrice = np.array(price['High'])
    return HighPrice

def GetLowPrice(price):
    LowPrice = np.array(price['Low'])
    return LowPrice

def GetDif(price,n):
    ma = MA(price,n)
    #AVP = GetAVP(price)
    dif = ma-price
    return dif

def GetRange(price):
    r = GetReturn(price)
    r = abs(r)
    rang = r.sum()/len(r)
    return rang

def isSwing(price1,price2,DPrice,n,SwingCon=1.2):

    dif = GetDif(price1,n)
    if(len(DPrice)>0):
        ExClose = float(DPrice['Close'])
    else:
        ExClose = price2[0]
    #if (dif.sum()<0 and len(price1)>30):
    #    return True
    difWeight = np.array(range(len(dif)))
    # dif = (10000 * abs(dif.sum()) / price1[-1]+SwingCon)/len(dif) #平均差分的百分比
   
    #multi_dif = dif*difWeight
    dif = (10000 * abs(max((dif*difWeight).sum(),0)) / price1[-1]+15 * SwingCon)/(difWeight.sum())
    #print('dif=',dif,' ',end='')
                       ### 小于0，均线以上算作0
    ####### 跌幅惩罚 ##### ####################
    ret = (price1[-1]/ExClose -1) * 100  # 涨跌幅，改为用昨收计算涨跌幅
    currvol = max( 100000 * pow(abs(min(ret,0)),1),1)
    

    ###### 涨幅奖励  ###########################
    #currvol /=  max(10000 * pow(max(ret,0),1),1)
    currvol /=  max(100000 * pow(int(ret>0),1),1)
    
    ### 压力位惩罚（当前回撤）#############
    HP = price1.max()
    drawDown = (HP/price1[-1] -1) *100
    currvol  *= 1000. * pow(10,int(max(1,drawDown)))
    
    ####  时间衰退###################
    # currvol  = currvol/math.log(max(len(price1),10)) * math.log(10)  #此处的log实际为ln
    # currvol *= 0.01*pow(30./max(len(price1),30),8)
    currvol *= 1*pow(10,int(-max(len(price1),30)/30))
    ################################################
    
    ###### 支撑位奖励 #####################
    #
    # to be continued....
    #####################################
    
    ###### 创新低惩罚 ###################
    # 近30分钟低于此前最低价的比例 
    
    perN = 30
    if len(price1)>perN:
        last_N_price = price1[max(len(price1)-perN,0):len(price1)]
        before_price = price1[0:max(len(price1)-perN,0)]
        min_price = before_price.min()
        lowNum = (last_N_price<(min_price*0.997)).sum()
        currvol *= 100*max(lowNum-10,1)
    ##################################
    
    
    dif = dif * currvol
    if dif < rang:
        return True
    else:
        return False
   

'''
def isMech(price,n):
    dif = GetDif(price,n)
    for i in dif:
        if abs(i) > 0.5:
            return True
'''

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

def ShowBP(price,BP,ss=5,cl='g'):   # 输出显示买点
    #plt.figure()
    #plt.scatter([10,],[28,],s=50,color='g')
    #plt.title()
    for i in BP:
        x = i+1
        y = price[i+1]
        plt.scatter([x,],[y,], s=ss, color=cl)
    return      

    
def GetBuyLowP(price,DPrice,index_data,N=5,MinBP=10,SwingCon=1.2,BuyOrSell = 'Buy',MaxBuyTimes = 3):      # 技术指标计算买点
    #price = SQLCode.GetPrice()
    OpenPrice = GetOpenPrice(price)
    ClosePrice = GetClosePrice(price)
    HighPrice = GetHighPrice(price)
    LowPrice = GetLowPrice(price)
    Var1 = ClosePrice - LLV(LowPrice,N)
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
    '''
    plt.figure(2)
    plt.title('S1,S2')
    plt.plot(S1)
    plt.plot(S2)
    '''
    CP = CrossPoint(S1,S2).T
    CP3 = np.argwhere(Var7<10).T  
    BP = np.intersect1d(CP,CP3)
    # print(BP)
    #ShowBP(ClosePrice,BP,50,'r')
    
    # 最小买入时间判断：
    BP = BP[BP>=MinBP]  ## 最早交易时间，卖出为3分钟，买入为30分钟
    # 收盘价过滤
    BP = BP[BP<240]
    BP.sort()
    
    ############# 支撑位买入 #############################
    #if(BuyOrSell == 'Buy'):
    

    ########### 低波动率买入 ###############################
    if(BuyOrSell == 'Buy' and LowVolBuyOn):
        N_LowVol = 60         # 计算周期         
        vol_n = 1.5             # 最低波动率
        hh = HHV(HighPrice,N_LowVol)
        ll = LLV(LowPrice,N_LowVol)
        N_range = (hh/ll -1) *100
        LowVolPoint = N_range[N_range<vol_n]
        LV_BP = np.array(LowVolPoint.index)
        LV_BP = LV_BP[LV_BP>N_LowVol]
        BP = np.append(BP,LV_BP)
        BP = np.array(BP,dtype=int)
        BP = BP[BP>=MinBP]
        BP = BP[BP<240]
        BP.sort()
    #############################################################
    
    
    
    
    ############## 0、用指数调整参数 ################################
    DownNum = - 0.8 + np.zeros(240)
    if(BuyOrSell == 'Buy' and IndexOn):
        zz500 = index_data[0][1]               # 399905 中证500深指当日股价 
        zz500_preDay = index_data[1].iloc[1]   # 中证500深指 前一日高开低收
        zz500_ExClose = zz500_preDay['Close']
        zz500 = zz500[1]
        close_zz500 = zz500['Close']
        ret_index = (close_zz500 /zz500_ExClose-1)*100  # 中证500开盘后的涨跌幅
        
        # index_drawDown = 
        # 动态的下跌参数
        DownNum += 1.*ret_index              
        
    ###############################################################

    
    ######### 1、买入加入Swing过滤，对第一个买点做震荡判定 ##########################################
    if(BuyOrSell == 'Buy' and SwingOn):
        if(len(BP)>0):
            #for i in range(len(BP)):
            #for i in range(len(BP)):   # 只对第一个买点做震荡过滤
            i=0
            temPrice1 = ClosePrice[0:BP[i]]
            temPrice2 = OpenPrice[0:BP[i]]
            iss = isSwing(temPrice1,temPrice2,DPrice,SwingN,SwingCon=SwingCon)  # 判断是否为震荡 True为震荡
            if (not iss) : 
                BP[i] = -1
                        # print('Swing Delete')
            BP = BP[BP>0]
    #ShowBP(ClosePrice,BP,70,'b')         ## 红色为所有买入点，震荡过滤后为蓝色点，机械点过滤后为绿色买入点
    
    ### ?? 先后顺序对业绩影响的测试
    ########### 2、买入加入机械点过滤 ##################################
    if(BuyOrSell == 'Buy' and MechOn):
        if(len(BP)>0):
            avp = GetAVP(price)
            dif = avp - ClosePrice
            dif = 100 * dif / ClosePrice
            for i in range(0,len(BP)):
                if(dif[BP[i]]<MechPoint): BP[i] = -1
            BP = BP[BP>0]
        # print(BP)
    # 判断每日最大买入次数
    #ShowBP(ClosePrice,BP[0:min(len(BP),MaxBuyTimes)],80,'g')
    ##################################################################
    
    
    #######  3、 第六版： 加入线性买点 ####################################
    if(BuyOrSell == 'Buy' and LinearBuyOn == True):
        avp = GetAVP(price)
        LinearN = 90
        #tmp = price[LinearN:]
        #tmp_avp = avp[LinearN:]
        DymMechLine = avp - avp*0.005*(300-pd.Series(avp.index))/60
        #dif = ClosePrice / DymMechLine -1  # 与直线的价差
        #dif = dif[LinearN:]    # 截取90分钟，开始算起
        #BelowDym = dif[dif<0]  # 股价低于线性价的
        dif2 = (ClosePrice / avp -1)*100  # 与均线的价差百分比
        #BelowDym = BelowDym[dif2>-2]   # 距离均价不超过2%
        CSP = CrossPoint(ClosePrice,DymMechLine)   # 股价向下穿过DymMechLine
        Con1 = np.array(dif[dif2>-2].index)
        CP = np.intersect1d(CSP,Con1)
        CP = CP[CP>LinearN]          # 去掉90分钟之前
        #CSP = CSP[dif2>-2]
        BP = np.append(BP,CP)  # 合并买点
        BP = np.unique(BP)     # 去重复
        BP.sort()              #排序，按照时间先后
    #
    ##############################################################
    
    

    ########## 4、跌幅过滤 ############################################
    if(len(DPrice)>0 and DownOn == True):
        ExClose = float(DPrice['Close'])
        #print(ExClose)
    else:
        ExClose = ClosePrice[0]
    if(BuyOrSell == 'Buy'):
        if(len(BP)>0):
            for i in range(len(BP)):
                ret = (ClosePrice[BP[i]]/ExClose-1)*100
                avg_ret = ret * 60. / (max(BP[i],60))  # 根据交易时间控制在-0.6%~-3.2%以内
                if (avg_ret<DownNum[BP[i]]):   # 跌幅参数，默认为-0.8
                    BP[i] = -1
                    #print('Slip Speed Delete')
            BP = BP[BP>0]
    ###############################################################
    
    ##  第六版 #################
    ########## 5、冲高回落过滤 ############################################
    if(BuyOrSell == 'Buy'):
        if(len(BP)>0):
            for i in range(len(BP)):
                tmp = ClosePrice[max(BP[i]-30,0):BP[i]] # 一小时内的收盘价
                HP = tmp.max()
                drawDown = (HP/ClosePrice[BP[i]] - 1)*100
                if(drawDown > 4):
                    BP[i] = -1
            BP = BP[BP>0]
    ###################################################################
    

    
    ###########  5、买入，整体低于均价过滤 ############################
    ## 若70%的时间股价低于均价，并且是下跌趋势，则不买入 ######################
    if(BuyOrSell == 'Buy' and BelowAVPOn):  
        if(len(BP)>0):
                #temPrice = ClosePrice[0:BP[i]]
                #iss = isSwing(temPrice,SwingN,SwingCon=SwingCon)  # 判断是否为震荡 True为震荡
                #if (not iss) : BP[i] = 0
            avp = GetAVP(price)
            dif = ClosePrice/avp -1
            for i in range(len(BP)):
                tmp = dif[0:BP[i]]
                belowNum = tmp[tmp<0]  # temp<0 在均线一下
                belowRatio = len(belowNum)/len(tmp)
                # avp_ret = (avp[BP[i]]/ExClose-1)*100
                avp_ret = (ClosePrice[BP[i]]/ExClose-1)*100
                avp_ret *= (60./(BP[i]))
                if ((belowRatio>0.8 and avp_ret<-0.4) or belowRatio>0.9):   #70%的时间在均价以下，均值跌幅超过0.1，不抄底 
                    BP[i] = -1
                    #print('Below AVP Delete')
            BP = BP[BP>0]
     #############################################################       
     


    
    ########### 7、 5分钟k线持续下跌延迟 ##################################
    if (BuyOrSell == 'Buy' and DownTrendDelay):   # 考虑多点过滤改成多点延迟
        N1_delay = 10
        #N2_delay = 10
        if(len(BP)>0):
            for j in range(len(BP)):
                if( j<(len(BP)-1)):
                    tmp = ClosePrice[BP[j]:BP[j+1]]
                    tmp_delay =  ClosePrice[max(BP[j]-N1_delay,0):BP[j+1]-N1_delay]
                    #tmp_delay2 = ClosePrice[max(BP[j]-N2_delay,0):BP[j+1]-N2_delay]
                else:
                    tmp = ClosePrice[BP[j]:]
                    tmp_delay = ClosePrice[BP[j]-N1_delay:-N1_delay]
                    #tmp_delay2  = ClosePrice[BP[j]-N2_delay:-N2_delay]
                # tmp_BP = np.argwhere(tmp<(lastBuyPrice*(1-BuyStepRange*0.01))).T
                # tmp_BP = np.argwhere((tmp>tmp_delay) * (tmp>tmp_delay2)).T
                tmp_BP = np.argwhere((tmp>=tmp_delay)).T              #股价不低于5分钟前价格买入
                tmp_BP = tmp_BP[0]
                if(len(tmp_BP)>0):
                    BP[j] = tmp_BP[0]+BP[j]  #第一个触发机械点位的买入
                else:
                    if(BP[j]<239):        # 最后一分钟除外
                        BP[j] = -1      # 如果没有则删除 BP[j]
            BP = BP[BP>0]      
    ################################################

    ########### 8、支撑位延迟买入 #################################
    if (BuyOrSell == 'Buy' and SupportDelay):   # 考虑多点过滤改成多点延迟
        i=0
        #
        #
        #
        #
        #
        #
        #
        #
        #
        #
    ################################################
    
    ######## 9、创新低延迟买入 #########################
    
    
    
    
    
    
    ################################################


    ########### 9、 机械点位延迟 ##################################
    if (BuyOrSell == 'Buy' and MultiDelayOn):   # 考虑多点过滤改成多点延迟
        BuyStepRange = 0.5
        if(len(BP)>1):
            lastBuyPrice = OpenPrice[BP[0]+1] ## 第一次买入价
            for i in range(len(BP)-1):
                j=i+1
                if( j<(len(BP)-1)):
                    tmp = ClosePrice[BP[j]:BP[j+1]]
                else:
                    tmp = ClosePrice[BP[j]:]
                tmp_BP = np.argwhere(tmp<(lastBuyPrice*(1-BuyStepRange*0.01))).T  #低于上一次买价一个机械点位
                tmp_BP = tmp_BP[0]
                if(len(tmp_BP)>0):
                    BP[j] = tmp_BP[0]+BP[j]  #第一个触发机械点位的买入
                    lastBuyPrice = OpenPrice[BP[j]+1]
                else:                       
                    BP[j] = -1      # 如果没有则删除 BP[j]
            BP = BP[BP>0]      
    # ????? 计算过程问题
    ################################################
    
    


    #################################################################
    ########### 10、  加入多点买入过滤系统 ########################
    if (BuyOrSell == 'Buy' and MultiOn):   # 考虑多点过滤改成多点延迟
        BuyStepRange = 0.5
        if(len(BP)>1):
            ###### 买点价格低于上一买点价格 0.5%以上#############
            lastBuyPrice = OpenPrice[BP[0]+1] ## 第一次买入价
            for i in range(len(BP)-1):
                    # lastBuyPrice = OpenPrice[BP[i]+1]    # 上一次买入价
                if (ClosePrice[BP[i+1]]/lastBuyPrice)>(1-BuyStepRange*0.01):
                    BP[i+1] = -1  #  -1标记为待删除项
                else:
                    lastBuyPrice = OpenPrice[BP[i+1]+1]  # 后面的最新买入价
                
                # 此前代码：
                #if (ClosePrice[BP[i+1]]/OpenPrice[BP[0]+1])>(1-BuyStepRange*0.01):
                    #BP[i+1] = -1  #  -1标记为待删除项

                     # print('BuyStepRange Delete')
            BP = BP[BP>0]
    ##############################################################
            
            

    
    
            # ##### 放量下跌趋势不再买入 ####################
            # for i in range(1,len(BP)):
            #     if(BP[i]>10):    
            #         tmpPrice = ClosePrice[BP[i]-5:BP[i]]
            #         GetReturn(tmpPrice)  
            # ###################################################
            
            # 限定最多买入次数
            BP = BP[0:MaxBuyTimes]
    ###### 加入冲高回落高抛点 #############################
    ## 冲高横盘或者冲高回撤

    ######################################################
    BP.sort()
    ### 画出买点 #####################
    if (len(BP)>0):
        ShowBP(ClosePrice,BP,5,'g')
    return BP
    #################################
    
    


def Filter1(price,n):
    return True;

def UpCross(a,b):
    return True;

def DownCross(a,b):
    return True;


        
'''
if __name__ == '__main__' :

    ar = 2*(np.random.rand(50)-0.5)
    ar = ar.cumsum()+20
    print(ar)
    
    ar_ma = MA(ar,10)
    print(ar_ma)
    n = 3
    r = GetReturn(ar)
    dif = GetDif(ar,n)
    plt.plot(ar)
    plt.plot(ar_ma)
    print("是否震荡:",isSwing(ar,3))

    #plt.plot(r)

'''


