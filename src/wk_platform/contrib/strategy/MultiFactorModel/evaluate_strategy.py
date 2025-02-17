# -*- coding: utf-8 -*-
"""
Created on Mon Nov 06 15:28:10 2017

@author: yangr
"""

import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats
import math
import copy
import os


def evaluate_strategy():
    columns = ['最大回撤','年化收益率','夏普比率']
    path = "dataFile/result/" 
    files= os.listdir(path)
    print(files)
    #得到文件夹下的所有文件名称  
    result_combine = pd.DataFrame()
    for f in files: #遍历文件夹 
        print(path+f)
        temp_file = pd.read_excel(path+f,'策略指标')
        
        temp_file = temp_file[columns]
        
        temp_file = temp_file.ix[0]
        temp_file = temp_file.rename(f)
        print(temp_file)
        result_combine = result_combine.append(temp_file)
        
    """
    最大回撤从小到大排序，其他两个从大到小排序
    """
    
    column = columns[0]
    result_combine[column+'rank'] =  result_combine[column].rank()
    
    for column in columns[1:]:
        
        result_combine[column+'rank'] =  result_combine[column].rank(ascending = False)
        
    result_combine['final_rank'] = result_combine[[column+'rank' for column in columns]].mean(1)
    
    print(result_combine)
    
    result_combine.to_csv('dataFile/result_combine.csv', encoding = 'gbk')
    
    
    
    
if __name__ == "__main__":
    
    evaluate_strategy()