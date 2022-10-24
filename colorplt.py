# -*- coding: utf-8 -*-
"""
Created on Wed Aug  3 15:41:04 2022

@author: MC
"""

import numpy as np
import matplotlib.pyplot as plt
import pymysql
import pandas as pd
import func1
import time
import datetime
from math import *
import os
from PIL import Image

def getprobs(x, sigma, mu):
    return 1/(sqrt(2*pi)*sigma)*exp(-(x-mu)**2/(2*sigma**2))
x = np.linspace(-8, 8, 100)
sigma = 2
mu = 0
y = getprobs(x, sigma, mu)
plt.plot(x, y, 'b')

low = mu-2*sigma
high = mu+2*sigma

# (mu-2sigma，mu+2sigma)
normal = linspace(low,high,20)
normal_y = getprobs(normal,sigma,mu)
plt.fill_between(normal, normal_y, color='g', alpha=0.7)

# （-8，mu-2sigma）
abnormal_low = linspace(-8,low,5)
abnormal_low_y = getprobs(abnormal_low,sigma,mu)
plt.fill_between(abnormal_low, abnormal_low_y, color='r', alpha=0.7)

# （mu+2sigma，8）
abnormal_high = linspace(high,8,5)
abnormal_high_y = getprobs(abnormal_high,sigma,mu)
plt.fill_between(abnormal_high, abnormal_high_y, color='r', alpha=0.7)

plt.xticks([]) # 不显示x轴y轴
plt.yticks([])
plt.show()


