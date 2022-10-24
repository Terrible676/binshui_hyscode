# -*- coding: utf-8 -*-
"""
Created on Mon Sep  5 14:26:34 2022

@author: MC
"""
import tushare as ts

ts.set_token('e5ec674a51275b8667adf454d34b5a078ba269f696e51b65277c4dfd')

pro = ts.pro_api()

df = pro.trade_cal(exchange='', start_date='20220901', end_date='20220902', fields='exchange,cal_date,is_open,pretrade_date', is_open='0')
