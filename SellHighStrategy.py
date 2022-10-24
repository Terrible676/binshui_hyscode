# -*- coding: utf-8 -*-
"""
Created on Fri Aug  5 11:01:44 2022

@author: MC
"""

import Strategy1
import pandas as pd
import numpy as np
import copy
import datetime



'''
def BackTest(Stock_List,df_list,DDate,MinBP=10,RandTest=False):
    return
'''

if __name__ == '__main__' :
    
    ########### 获取股票池 ######################################
    # stock_list = Strategy1.GetStockList()
    
    rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0902.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    rd_j = rd_excel['代码']
    stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8]))
    stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    ### 
    ### 用原始策略回测季总持仓
    
    
    #stock_list= pd.Series(['m1_002537'])
        ########### 下载数据 ############################################
    #BTDate = ['2022-08-19','2022-08-22','2022-08-23','2022-08-24']
    #BTDate =['2022-08-15','2022-08-16','2022-08-17','2022-08-18','2022-08-19','2022-08-22','2022-08-23','2022-08-24','2022-08-25','2022-08-26']   # 15-26号
    BTDate = ['2022-09-02']
    for DDate in BTDate:
        testNum = 3000
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
        ###############################################################
        
        # #################
        # #计算 10:23前的数据
        # cut_df_list = copy.deepcopy(df_list)
        # #cut_df_list = pd.Series(cut_df_list)
        # #df = copy.deepcopy(df_list[0])
        # #a = df[1][0:100]
        # #cut_df_list = cut_df_list.apply(lambda x:np.array(x[0],x[1][0:100]))
        # n=100
        # for i in range(len(cut_df_list)):
        #     if(len(cut_df_list[i])>0):
        #         tmp =  copy.deepcopy(cut_df_list[i])
        #         #tmp = pd.DataFrame(cut_df_list[i][0],cut_df_list[i][1][0:n])
        #         tmp[0] = cut_df_list[i][0]
        #         tmp[1] = cut_df_list[i][1][0:n]
        #         cut_df_list[i] = tmp
        # ##############################################################
    
        
        
        ###### 加入时间惩罚 #######################################
        # 策略优化中卖得越早收益越高，
        # 或，时间越晚卖出的条件越宽松
        # 不再值得持有的时候就卖出
        # 长时间价格盘整则卖出
        # 长时间成交量较低则卖出
        #############################################################
            
        ########## 回测RevData ##############################
        ReData = np.array(Strategy1.DataReverse(df_list))  # Buy用df_list Sell 用ReData
        buyorsell = 'Buy'
        minBP = 3
        if(buyorsell == 'Buy'):
            minBP = 30                                     # 前30分钟不买
        output = Strategy1.BackTest(df_list,               # Sell ReData, Buy 用df_list
                          DayData,
                          index_data,
                          DDate,
                          MinBP= minBP, 
                          SwingCon = 1,
                          RandTest = False,
                          TestNum=testNum,
                          BuyOrSell=buyorsell,    # Sell: 把 MechOn、SwingOn 改为 False  
                          MaxBuyTimes = 3    
                          )
        ######## 绘制并保存卖点图片 ###################
        '''
        Strategy1.SavePic(output,
                          df_list,
                          DayData,
                          DDate,
                          SwingCon=1,
                          BuyOrSell = buyorsell,   # Buy、Sell
                          inver = False,
                          MaxBuyTimes=3
                          )
        '''
        
        ##############################################
        
        BPTime = output['买入时间']
        BPTime = BPTime.apply(lambda x:Strategy1.GetBPTime(int(x)))
        prt_output = copy.deepcopy(output)
        prt_stock =  prt_output['股票代码']
        prt_output['股票代码']=prt_stock.apply(lambda x:x[3:9])
        prt_output['买入时间']=BPTime
        prt_output.to_excel('5MBT%s output.xlsx' %DDate)
        
    
    
    #plt.plot(aa)
    #plt.gca().invert_yaxis()



    