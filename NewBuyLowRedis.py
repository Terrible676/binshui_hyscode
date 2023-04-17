# -*- coding: utf-8 -*-
"""
Created on Fri Apr 14 15:47:23 2023

@author: MC
"""

import redis
from time import sleep
import json
import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import os
import func1

def GetAVP(ClosePrice,VolData):
    tmp = ClosePrice*VolData
    cs = tmp.cumsum()
    vcs = VolData.cumsum()
    AVP = cs/vcs
    return AVP

#####价格支撑买入##############################
def isSupport(close_data,high_data,low_data,open_data,vol_data,
               ExData,LastBuyPrice,Ret_index = 0,n=30):
    P1 = False
    try:
        ClosePrice = np.array(close_data)
        LowPrice =  np.array(low_data)
        HighPrice =  np.array(high_data)
        OpenPrice =  np.array(open_data)
        VolData = np.array(vol_data)
        ExClose = ExData['close']
        ExVol = ExData['volume']
        #ExLow = ExData['Low']
        ret = (ClosePrice[-1]/ExClose -1) * 100
        ret_rlt = ret - 2*Ret_index
        ret_cur =  (ClosePrice[-1]/OpenPrice[0] -1) * 100
        avp = GetAVP(ClosePrice,VolData)
        # avp = func1.GetAVP(price)
        '''
        ##### 支撑价 ###############
        Supporting = ( ClosePrice[-1]/LowPrice.min() -1) * 100
        Speed_Sup = Supporting/ (max(len(ClosePrice)/10,1))
        ##############################
        '''
        
        ##### DrawDown ################
        DrawDown = ( HighPrice.max()/ClosePrice[-1] -1) * 100
        
        if(LastBuyPrice<0):
            isMech = ClosePrice[-1]/avp[-1] < 0.995
        else:
            isMech = ClosePrice[-1]/LastBuyPrice < 0.995
            
        if(not(isMech)):
            return False            
        if( DrawDown>2.5 ):
            return False
        # plunge
        nearAmount = VolData[-3:].mean()
        plg = nearAmount/(VolData.mean())
        if( plg > 1 ):
            return False
        ##########  GoldSpt#######################
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
        #Ratio3 = (HH/LL -1) *100
        GoldSpt = (max_index>min_index) and Ratio1<0.9 and Ratio2<0.8
        #######################################
        
        if(GoldSpt ):
            return True
    except:
        P1 = False
        return P1
    return P1
##########################################################



def min_to_time(mnt):
    # 获取当前时间
    today = datetime.date.today()
    n = mnt
    #中午休息90分钟
    if(mnt>120):
        n += 90
    # 设定目标时间
    target_time = datetime.datetime.combine(today, datetime.time(hour=9, minute=30)) + datetime.timedelta(minutes=n)
    
    # 将时间转换为指定格式
    target_time_str = target_time.strftime('%Y-%m-%d %H:%M:%S')    
    return target_time_str
    
##########################################################
def GetLastDayData():
    # 取前一日数据
    print('Getting Last Day Data.')
    r0 = redis.Redis(host='192.168.20.200',
                    port=6379,
                    password='ji666888',
                    db=0)
    d_stock = r0.keys()
    day_data = {}
    for stock in d_stock:
        try:
            data = r0.get(stock)
            data_str = data.decode('utf-8')
            # 解析 JSON 字符串为 Python 数据结构
            data_list = json.loads(data_str)
            # 获取第一个数据
            first_data = data_list[0]
            #day_data = json.loads(day_data)
            day_data[stock.decode()] = first_data
            
        except:
            #print('NoDayData:',stock,end='')
            continue
    r0.close()
    print('LastDay Data Downloaded.')
    return day_data
'''
old_json_str = '{"key1": "value1", "key2": "value2"}'
old_json = json.loads(old_json_str)  # 将 JSON 字符串转换为 Python 对象
old_json["new_key"] = "new_value"  # 插入新项
new_json_str = json.dumps(old_json)  # 将 Python 对象转换为 JSON 字符串
print(new_json_str)    
'''

def GetLastMnt(minutes):
    mnt_int = [int(x) for x in minutes]    
    mnt_int =  list(filter(lambda x: x <= 250, mnt_int))
    last_mnt = max(mnt_int)
    return last_mnt
    
def to_redis(rr,SheetName,stock,mnt,buyprice):
    # 写入数据库
    ##获取redis时间戳
    timestamp = rr.time()[0]  
    dt = datetime.datetime.fromtimestamp(timestamp)
    str_time = dt.strftime('%Y-%m-%d %H:%M:%S')    
    try:
        EstData = rr.get(SheetName)
        if(EstData):
            EstData = json.loads(EstData)
        # buytime = min_to_time(mnt)
        BuyData = {"Code":stock,"bargain_price":buyprice,"time":str_time}
        if(EstData):
            value_to_write = EstData+[BuyData]
        else:
            value_to_write =[BuyData]
        value_to_write = json.dumps(value_to_write)
        rr.set(SheetName,value_to_write)
    except:
        print('to_redis failed.',end='')
    return

def SaveExcel(output):
    if(len(output)>0):
        DDate = datetime.datetime.now()
        DDate = DDate.strftime('%Y-%m-%d')    
        df = pd.DataFrame({k: pd.Series(v) for k, v in output.items()})
        df = df.T
        df.columns = ['买入时间','买入价格']
        df.index.name = '股票代码'
        os.makedirs(r'F:\hyscode\NewBuyLow\output', exist_ok=True)
        df.to_excel('F:/hyscode/NewBuyLow/output/多点低吸7第一点_%s output.xlsx' %DDate)
        print('EarlyChase_',DDate,' output',' Saved.')
    return

def main():
    # 连接Redis服务器
    r = redis.Redis(host='192.168.20.200',
                    port=6379,
                    password='ji666888',
                    db=2)
    # config = r.config_get()
    
    ####预处理前一日数据#############
    DayData = GetLastDayData()
    InxDayData = DayData.get('399905')  #中证500前日数据
    
    ##### 读取市值数据  ########################################################
    read_Capital = pd.read_excel(r'F:/hyscode/CapitalData2.xlsx',dtype=object)
    dm = read_Capital['代码']
    dm = dm.apply(lambda x:x[2:8])
    CapitalData = pd.DataFrame([dm,read_Capital['总市值']]).T
    CapitalSeries = CapitalData.set_index('代码')['总市值'].squeeze()
    CapitalDict =  CapitalSeries.to_dict()
    ########### 下载数据 ############################################

    ######### 预处理写入数据库 ################
    rr = redis.Redis(host='192.168.20.200',
                    port=6379,
                    password='ji666888',
                    db=5)
    td = datetime.datetime.now()
    td = td.strftime('%Y%m%d')
    #td = str(td.date())
    SheetName = 'he_early_chase_'+td + 'backtest'
    # SheetData2 = rr.get('he_fakefell_buy_20221116')
    '''
    SheetData = rr.get(SheetName)
    if(SheetData):
        print(1)
    else:
        print(0)
        rr.set(SheetName,'') # 创建一个空的键值
    '''
    #############################################

    #### 预设变量 #####################
    index_list = ['399001','399005','399905','999999','399006','399300']
    last_count = 0
    data = {}
    TotalBuy = 0 
    PnL = 0
    bought_list = []
    output = {}
    last_dict = {}
    #################################################
    
    for mnt in range(230):
    #while(True):
        # db=0 日K数据
        # db=2 分钟数据,keys是时间标记
        #info = r.info()
        minutes = r.keys()
        mnt_count = len(minutes)
        #if(mnt_count>6 and mnt_count>last_count):  #每新增加一分钟数据，开始运行一次
        #    mnt = GetLastMnt(minutes) #计算最新一分钟
        if(True):
            print('Calculating Started.')
            last_count = len(minutes)
            print('last_count:',last_count)
            if(mnt>=0): #实盘
                ##### 读取每一分钟数据################################
                values = r.get(mnt)
                '''
                timestamp = r.time()[0]  
                now = datetime.datetime.fromtimestamp(timestamp)
                print(now)
                '''
                #print(len(minutes))
                data_dict = json.loads(values.decode('utf-8'))
                ########################################################
                
                ##### 合并数据到data中###################################
                for d in [data_dict]:
                    for k, v in d.items():
                        if k in data:
                            tmp = [v]
                            data[k] = data[k]+tmp  #list(data[k],v])
                        else:
                            tmp = [v]
                            data[k] = tmp
                #######################################################

                ##### 获取股票池名单 ###################
                stock_list = list(data_dict.keys())
                stock_list = list(set(stock_list)-set(index_list)) #去掉指数代码
                ################################
                # 计算 Ret_index ############
                try:
                    ExInx = InxDayData.get('close')
                    CurInx = data_dict.get('399905')
                    CurInx = CurInx.get('close')
                    Ret_index = (CurInx/ExInx -1)*100
                except:
                    Ret_index = 0
                
                #######################
                if(mnt<30): #30分钟以内只读数据不交易
                    continue
                for stock in stock_list:
                    if(stock in bought_list):
                        continue
                    try:
                        ExData = DayData.get(stock)
                        code_data = data.get(stock)  # 取出个股当前所有分钟数据
                        try:
                            Capital = CapitalDict[stock]  #读取市值数据
                        except:
                            Capital = 10000000000
                        # 整理数据结构
                        if code_data:
                            # 获取日期时间
                            #date_time = code_data.get('date_time')
                            #open_price = code_data.get('open')
                            #print('股票代码:', stock)
                            close_data = [d['close'] for d in code_data]
                            high_data = [d['high'] for d in code_data]
                            open_data = [d['open'] for d in code_data]
                            low_data = [d['low'] for d in code_data]
                            # amount_data = [d['amount'] for d in code_data] #amount数据不准，用volume数据
                            vol_data = [d['volume'] for d in code_data]
                            #df = pd.DataFrame.from_records(code_data, columns=['open', 'close', 'high'])     
                        else:
                            print('未找到股票代码:', stock)
                            continue
                        
                        LastBuyPrice = -1 #只买第一点
                        ### 交易判断 ###############################
                        P1 = isSupport(close_data,high_data,low_data,open_data,vol_data,
                                       ExData,LastBuyPrice,Ret_index = 0,n=30)
                        ##########################################
                        if(P1):
                            print(stock,'',mnt,'',end='')
                            buyprice = close_data[-1]
                            bought_list += [stock]
                            TotalBuy += 1
                            #last_dict[stock] = {'LastBuyPrice':buyprice,'BuyTimes':}
                            output[stock] = [min_to_time(mnt+1)[11:16],buyprice]
                            # to_redis(rr, SheetName, stock, mnt+1, buyprice)
                    except:
                        continue
                '''
                timestamp = r.time()[0]  
                now = datetime.datetime.fromtimestamp(timestamp)
                print(now)
                '''      
        #sleep(0.2)
        #print('Waiting Redis update. ',end='')

    
    r.close()
    rr.close()
    print('')
    print('多点低吸7:',td)
    print('TotalBuy:',TotalBuy)
    SaveExcel(output)
    return

if __name__ == '__main__' :
    
    main()
        