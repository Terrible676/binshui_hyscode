# -*- coding: utf-8 -*-
"""
Created on Tue Apr 11 11:01:16 2023

@author: MC
"""


import datetime
td = datetime.datetime.now()
td = td.date()
Num = td.day
PrtKey = str(9979621319*(Num+1)) #input
f = open("PrtKey.txt", "w")
f.write(PrtKey)
f.close()