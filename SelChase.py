# -*- coding: utf-8 -*-
"""
Created on Tue Nov 15 11:13:59 2022

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
import pymysql


P1On = True
P2On = False
TurnNum = 1      #### 每分钟平均换手率，万一

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

##### 持续上涨追高 ##############################
def isSteadyRising(price,ExData,Capital,Ret_index = 0,n=30):
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
        
        
        if(GoldSpt and AboveRatio > 0.8 and TurnOver>0.5):
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

# def isStartUp():
    
def isLeading(price,tmp_index):
    try:
        if():
            
            return True
        else:
            return False
    except:
        return True
    return True


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


if __name__ == '__main__' :
    
    ########### 获取股票池 ######################################
    # stock_list = Strategy1.GetStockList()
    
    #rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0921.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    # rd_j = rd_excel['代码']
    #stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8]))
    #stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    stock_list = pd.Series(GetAllStockList())
    stock_list2 = stock_list.apply(lambda x:'d_'+(x[3:9]))
    ### 用原始策略回测季总持仓
    ##### 读取市值数据  ########################################################
    read_Capital = pd.read_excel(r'F:/hyscode/CapitalData2.xlsx',dtype=object)
    dm = read_Capital['代码']
    dm = dm.apply(lambda x:x[2:8])
    CapitalData = pd.DataFrame([dm,read_Capital['总市值']]).T
    ########### 下载数据 ############################################
    #BTDate = ['2022-08-19','2022-08-22','2022-08-23','2022-08-24']
    BTDate =['2022-11-18']  
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
            df_list = Strategy1.GetDataList(stock_list,DDate,testNum)
            DayData = Strategy1.GetDayData(stock_list2,preDate,testNum)
            index_data = Strategy1.GetIndexData(DDate,preDate)
            #print(df_list)
        
        ## for bar backtest ############
        FirstBuyPoint = np.zeros(len(df_list),dtype = int)
        FirstBuyPrice = np.zeros(len(df_list))
        LastBuyPoint = np.zeros(len(df_list),dtype = int)
        LastBuyPrice = np.zeros(len(df_list))
        BuyTimes = np.zeros(len(df_list),dtype = int)
        BuyType = np.zeros(len(df_list),dtype = object)
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
                #BuyTimes = 0
                try:
                    Num = np.argwhere(np.array(CapitalData['代码'])==stock[3:9])[0][0]
                    Capital = CapitalData.iloc[Num][1]
                except:
                    Capital = 10000000000 # 没有数据则以100亿计算
                for j in range(1,len(PriceData)):
                    tmpPrice = PriceData[0:j+1]
                    tmp_index = ret_index[0:j+1]
                    # 1点半后的强势股买入
                    if(j>60 and j<234 and P1On):
                        P1 = isSteadyRising(tmpPrice,ExData,Capital,ret_index[j])
                        if(P1):
                            if(BuyTimes[i] == 0):
                                FirstBuyPoint[i] = int(j)
                                FirstBuyPrice[i] = PriceData['Open'][j+1]
                                # BuyType[i] = 'P1'
                                P1_Point += 1
                                PnL1 += ((np.array(PriceData['Close'])[-1])/FirstBuyPrice[i] -1)*100
                                print(stock,' P1',j,' ',end='')
                            LastBuyPoint[i] = int(j)
                            LastBuyPrice[i] = PriceData['Open'][j+1]
                            BuyTimes[i] += 1

                    '''
                    if(j>180 and j<234 and P2On):
                        P2 = Diver2Index(tmpPrice,tmp_index,Capital)  # 与大盘背离
                        if(P2):
                            BuyPoint[i] = int(j)
                            BuyPrice[i] = PriceData['Open'][j+1]
                            BuyType[i] = 'P2'
                            P2_Point +=1
                            PnL2 += ((np.array(PriceData['Close'])[-1])/BuyPrice[i] -1)*100
                            print(stock,' P2',j)
                            break
                    '''
                #if(BuyPoint[i]>0):
                #    PnL += (BuyPrice[i]/(np.array(PriceData['Close'])[-1]) -1)*100
            except:
                continue
           # r= Diver2Index(tmpPrice,tmp_index) 
           # if(r<0.1):
           # print(stock,' ',round(r,3))
                
        # print(BuyPoint)
        PnL = PnL1+PnL2
        Total_Buy = len(FirstBuyPoint[FirstBuyPoint>0])
        PnL /= max(Total_Buy,1)
        output = pd.DataFrame([FirstBuyPoint,FirstBuyPrice,LastBuyPoint,LastBuyPrice,BuyTimes]).T#,columns=['卖出时间','卖出价格'])
        output.index = StockName
        output.columns = (['买入时间','买入价格','最后买入时间','最后买入价格','买点次数'])
        
        
        ##### 下一交易日收盘收益率  #################################
        # output = output[output['买入时间']>0.1]
        # NextRet = GetNextDayRet(stock_list2,output,DDate,testNum)
        
        
        
        #############################################################
        
        print(DDate,' 追涨策略:')
        print('总股票数量：',len(df_list))
        print('买入股票数量:',Total_Buy)
        print('买入比例为：',round(Total_Buy/len(df_list)*100,3),'%')
        # print("P1买入数量:",P1_Point,' 收益率：',round(PnL1/max(P1_Point,1),3),'%')
        # print("P2买入数量:",P2_Point,' 收益率:',round(PnL2/max(P2_Point,1),3),'%')
        print('当日收益率：',round(PnL,3),'%')
        # print("次日收益率：",round(NextRet.mean()-PnL,3),'%')
        
        
        BPTime = output['买入时间']
        BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        LTime = output['最后买入时间']
        LTime = LTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        prt_output = copy.deepcopy(output)
        prt_stock =  pd.Series(prt_output.index)
        prt_output.index =prt_stock.apply(lambda x:x[3:9])
        prt_output['买入时间']=BPTime.values
        prt_output['最后买入时间']=LTime.values
        prt_output = prt_output[prt_output['买入价格']>0.1]
        prt_output.columns = (['第一买点','第一买价','最后买点','最后买价','买点次数'])
        print(prt_output)
        os.makedirs(r'F:\hyscode\Chase\output', exist_ok=True)
        prt_output.to_excel('F:/hyscode/Chase/output/选择性盘中追%s output.xlsx' %DDate)
        
        
        

        


