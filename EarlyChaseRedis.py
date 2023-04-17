# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 13:57:27 2023

@author: MC
"""

import redis
from time import sleep
import json
import pandas as pd
import numpy as np
from collections import defaultdict
import datetime
import pickle

##### 放量上涨 ##############################
def isPumpDown(close_data,high_data,open_data,low_data,vol_data,
               ExData,Capital,Ret_index=0):
    P1 = False
    try:
        ClosePrice = np.array(close_data)
        #LowPrice =  np.array(price['Low'])
        HighPrice =  np.array(high_data)
        OpenPrice =  np.array(open_data)
        ExClose = ExData['close']
        ExVol = ExData['volume']
        #ExLow = ExData['Low']
        ret = (ClosePrice[-1]/ExClose -1) * 100
        #ret_rlt = ret - 2*Ret_index
        ret_rlt = ret - 2*np.sign(Ret_index)*min(abs(Ret_index),1)
        DrawDown = (HighPrice.max() / ClosePrice[-1] -1) * 100
        cur_ret = (ClosePrice[-1] / OpenPrice[0] -1) * 100
        open_ret = (OpenPrice[0] /ExClose -1)*100
        # BelowAVP = dif[dif<0]
        # BelowRatio = len(BelowAVP)/len(ClosePrice)
        avg_Vol = np.array(vol_data).mean()
        avg_Amount = avg_Vol*ClosePrice[0] / 100. # 平均成交量(万)
        TurnOver = avg_Amount*100000000/Capital    # 每点代表一分钟换手率万分之一
        # print(TurnOver)
        if(ExVol < 1):
            return False
        isPump1 = (avg_Vol*240*(max(cur_ret,1)) / ExVol > 10)
        
        if( open_ret>-1 and
           open_ret< 4 and 
            (cur_ret >1) and   #(len(ClosePrice)>5) and 
            cur_ret< 3.5 and 
           (ret_rlt>0.5) and
           DrawDown < 1 and
           avg_Amount > 100 and   #平均成交量大于100万
           TurnOver > 0.5 and        
           isPump1
           ):
            P1 = True
            return P1
        ##########################################
    except:
        P1 = False
        return P1
    return P1

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

def GetLastMnt(minutes):
    mnt_int = [int(x) for x in minutes]    
    mnt_int =  list(filter(lambda x: x <= 250, mnt_int))
    last_mnt = max(mnt_int)
    return last_mnt

def to_redis(rr,SheetName,stock,mnt,buyprice):
    # 写入数据库
    timestamp = rr.time()[0]  
    dt = datetime.datetime.fromtimestamp(timestamp)
    str_time = dt.strftime('%Y-%m-%d %H:%M:%S')    
    try:
        EstData = rr.get(SheetName)
        if(EstData):
            EstData = json.loads(EstData)
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

def main():
    #### 连接Redis服务器 #########################
    r = redis.Redis(host='192.168.20.200',
                    port=6379,
                    password='ji666888',
                    db=2)
    # config = r.config_get()
    ##############################################
    
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
    ###########  ############################################
    
    ######### 预处理写入数据库 ################
    rr = redis.Redis(host='192.168.20.200',
                    port=6379,
                    password='ji666888',
                    db=5)
    td = datetime.datetime.now()
    td = td.strftime('%Y%m%d')
    SheetName = 'he_early_chase_'+td
    # SheetData2 = rr.get('he_fakefell_buy_20221116')
    '''
    SheetData = rr.get(SheetName)
    if(SheetData):
        print(1)
    else:
        print(0)
        rr.set(SheetName,'') # 创建一个空的键值
    '''
    ########################################
    # 盘中数据
    index_list = ['399001','399005','399905','999999','399006','399300']
    last_count = 0
    data = {}
    TotalBuy = 0 
    mnt = 0
    bought_list = []
    now = datetime.datetime.now()
    while(mnt<21 and now.hour<16):  #开盘后运行20分钟，并且时间在16:00之前（去掉不开盘的日期）
    #for mnt in range(20):
        # db=0 日K数据
        # db=2 分钟数据,keys是时间标记
        #info = r.info()
        minutes = r.keys()
        mnt_count = len(minutes)
        if(mnt_count>6 and mnt_count>last_count):  #每新增加一分钟数据，开始运行一次
            mnt = GetLastMnt(minutes) #计算最新一分钟
            print('Calculating Started.')
            last_count = len(minutes)
            print(last_count)
            if(mnt > 0 ): #实盘
            #for mnt in range(5): #回测
            
                ##### 读取每一分钟数据################################
                values = r.get(mnt)
                print(len(minutes))
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
                
                for stock in stock_list:
                    if(stock in bought_list): #买过的股票跳过
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
                            close_data = [d['close'] for d in code_data]
                            high_data = [d['high'] for d in code_data]
                            open_data = [d['open'] for d in code_data]
                            low_data = [d['low'] for d in code_data]
                            vol_data = [d['volume'] for d in code_data]
                        else:
                            print('未找到股票代码:', stock)
                            continue
                        
                        ### 交易判断 ##################################
                        if(ExData['close']<3):
                            continue
                        P1 = isPumpDown(close_data,high_data,open_data,low_data,vol_data,
                                        ExData,Capital,Ret_index)
                        ##########################################
                        if(P1):
                            print('ErlyChase ',stock,' ',mnt)
                            buyprice = close_data[-1]
                            bought_list += [stock]
                            to_redis(rr,SheetName,stock,mnt+1,buyprice)
                            TotalBuy += 1
                        ####################################################
                    except:
                        continue
                        
        sleep(0.2)
        # print('Waiting Redis update. ',end='')
    r.close()
    rr.close()
    print('TotalBuy:',TotalBuy)
    print('Waiting for next trading day..')
    return
    
if __name__ == '__main__' :
    
    ###每日9点开始运行##########
    print('EarlyChase Started:')
    while True:
        now = datetime.datetime.now()
        if now.hour == 9 and now.minute == 0 : #and now.second == 0: 9点钟整开始执行
            print("Starting controller...")
            main()
            # 在这里运行您的控制器程序
        else:
            # 等待1秒钟后再次检查时间
            print('\r','Waiting for next trading day..',now,'  ',end='')
            sleep(1)

    '''
    he_fakefell_buy_20230407
    [{"Code":"000016","bargain_price":5.29,"time":"2023-04-07 09:46:00","type":"1"},
     {"Code":"000021","bargain_price":19.460000000000001,"time":"2023-04-07 09:34:00","type":"1"}
     ]
    '''
    
    