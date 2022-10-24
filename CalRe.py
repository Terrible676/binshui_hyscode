# -*- coding: utf-8 -*-
"""
Created on Tue Sep  6 17:14:11 2022

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


if __name__ == '__main__' :

    rd_excel = pd.read_excel(r'F:/hyscode/RealStockList0907.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    rd_j = rd_excel['代码']
    stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8]))
    stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    
    DDate = '2022-09-07'
    testNum =3000
    #df_list = Strategy1.GetDataList(stock_list,DDate,testNum)
    DayData = Strategy1.GetDayData(stock_list2,DDate,testNum)
    
    BuyData = pd.read_excel('M0907.xlsx',dtype=object)
    PnL = 0.
    j=0
    for i in range(len(BuyData)):
        stock = BuyData.iloc[i][0]
        try:
            buyprice = float(BuyData.iloc[i][3])
        except:
            continue
        if(buyprice>0):
            stock = 'd_'+stock
            closeprice = DayData[DayData.index==stock]
            if(len(closeprice)>0):
                cp = float(closeprice['Close'][0])
            else:
                continue
            PnL += (cp/buyprice -1)*100
            j += 1
        else:
            continue
    print('j:',j)
    print('PnL:',PnL/j)
    
    
    
    
    