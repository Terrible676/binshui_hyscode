# -*- coding: utf-8 -*-
"""
Created on Wed Sep 14 18:16:53 2022

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


if __name__ == '__main__' :
    
    ########### 获取股票池 ######################################
    # stock_list = Strategy1.GetStockList()
    
    #rd_excel = pd.read_excel(r'F:/BuyLow2/output/_BuyLow2022-09-14 output.xlsx',dtype=object)
    #rd_excel = rd_excel[rd_excel['人']=='季']
    #rd_j = rd_excel['代码']
    #stock_list = rd_j.apply(lambda x:'m1_'+(x[2:8]))
    #stock_list2 = rd_j.apply(lambda x:'d_'+(x[2:8]))
    #stock_list = rd_excel['股票代码']
    
    df_hold = pd.read_excel(r'F:\BuyLow2\output\6MBuyLow2022-09-21 output.xlsx', converters={'股票代码': lambda x: str(x)})
    DDate ='2022-09-21'
    df_hold.columns = ['0','代码', '买入时间', '买入价']

    df_static = df_hold.groupby('代码').agg({'买入时间': 'size', '买入价': 'mean'}).rename(
    columns={'买入时间': '数量', '买入价': '均价'}).reset_index()
    df_static.loc[:, '均价'] = df_static.loc[:, '均价'].round(2)
    df_static.to_excel('F:/BuyLow2/output/H%s多点绝对均价.xlsx'%DDate)
    
    
    