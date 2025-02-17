# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 09:56:30 2017

@author: yangr

重组一致预期数据的因子
成长因子、估值因子、盈利因子、规模因子、情绪因子
earningData是一致预期盈利因子，共24个
growingAndValuationData是一致预期成长因子和一致预期估值因子，其中成长因子2个，估值因子20个
sizeAndEmoData是一致预期规模因子和一致预期情绪因子，其中规模因子5个，情绪因子12个

另外每个数据文件都在前面加上了市值、行业、交易量等基本指标。
"""

import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats
import math
import copy
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import datetime


def mergeData(datSelect,datForMerge):
        """
        为下面的合并做一些数据格式的处理
        """
        datSelect = datSelect.rename(columns = {"stock_code":"windcode", "con_date":"Date"})
        datSelect['Date'] = pd.to_datetime(datSelect['Date'],format = '%Y-%m-%d')
        datSelect['windcode'] = datSelect['windcode'].astype('str')
        
        
        """
        将原来的数据中的windcode去掉后面的SZ和SH，和forecast数据合并
        """
        def removeLast(obs):
            obs = str(obs)
            return obs[:6]
        
        
        
        datForMerge['windcode'] = datForMerge['windcode'].apply(removeLast)
    
        datForMerge['Date'] = pd.to_datetime(datForMerge['Date'],format = '%Y-%m-%d')
        
        datSelect = pd.merge(datSelect, datForMerge,how = 'inner',on = ['Date','windcode'])
        
        """
        处理一下异常值
        """
        datSelect[datSelect == -100] = np.nan
        
     
        
        print(datSelect)
        
        datSelect = datSelect.sort_values(by =['windcode','Date'], ascending = True)
        
        return datSelect
    
    
    

print('read data')
datForMerge = pd.read_hdf("/home/wk_bkt_public/platformData/priceAndFactorData/dataForRegressionPost.h5")
datForMerge = datForMerge[['Date','windcode','Volume','future_logreturn','MV','industry_name']]
datForMerge.rename(columns={'Volume':'volume'}, inplace=True)



forecastData1 = pd.read_hdf("/home/wk_bkt_public/platformData/forecast_data/forecast_data.h5",'clean_data')
forecastData2 = pd.read_hdf("/home/wk_bkt_public/platformData/forecast_data/forecast_der_alldata.h5",'clean_data')
forecastData3 = pd.read_hdf("/home/wk_bkt_public/platformData/forecast_data/forecast_rolling_data.h5",'clean_data')

print('end read data')


growingList = ['con_npcgrate_2y','con_npcgrate_2y_roll']
valuationList = ['con_pb','con_pe','con_peg','con_pb_mom','con_pe_mom','con_peg_mom','con_pe_roll','con_pe_roll_mom','con_pb_roll','con_pb_roll_mom','con_peg_roll','con_peg_roll_mom','con_pb_rank','con_peg_rank','con_pb_roll_rank','con_pe_roll_rank','con_peg_roll_rank','pe_dev_roll_75d','pb_dev_roll_25d','pb_dev_roll_75d']
earningList = ['con_npgrate_1w','con_npgrate_4w','con_npgrate_13w','con_npgrate_26w','con_roe_mom','con_eps_roll_mom','con_np_roll','con_np_roll_mom','con_npgrate_4w_rank','con_npgrate_13w_rank','con_npgrate_26w_rank','report_mean_np_w','report_mean_np_m','report_mean_np_q','report_max_np_w','report_max_np_m','report_max_np_q','report_min_np_w','report_min_np_m','report_min_np_q','eps_dev_75d','eps_std_75d','np_std_25d','np_std_75d']
sizeList = ['con_na','con_na_mom','con_na_roll','con_na_roll_mom','tcap_rank']
emoList = ['down_num_m','down_num_q','relative_report_num_75d','organ_num_25d','organ_num_75d','report_num_q','author_num_m','author_num_q','organ_num_m','organ_num_q','overweight_num_q','neutral_num_q']




print('merge data')
forecastData1 = mergeData(forecastData1, datForMerge)
forecastData2 = mergeData(forecastData2, datForMerge)
forecastData3 = mergeData(forecastData3, datForMerge)



forecastDataList = [forecastData1,forecastData2,forecastData3]

fundamental = forecastData1[['Date','windcode','MV','volume','industry_name','future_logreturn']]

growingAndValuationData = pd.DataFrame()
growingAndValuationData = fundamental
for foreData in forecastDataList:
    print(foreData)
    tempList = foreData.columns[foreData.columns.isin(growingList)].tolist()+foreData.columns[foreData.columns.isin(valuationList)].tolist()
    print(tempList)
    print(['Date']+['windcode']+tempList)
    df1 = foreData[['Date']+['windcode']+tempList]
    
    
    growingAndValuationData = pd.merge(growingAndValuationData,df1,how = 'outer',on = ['Date','windcode'])
    print(growingAndValuationData)

    growingAndValuationData.to_hdf('dataFile/data/growingAndValuationData.h5','df',complevel=9, complib='blosc')



sizeAndEmoData = pd.DataFrame()
sizeAndEmoData = fundamental
for foreData in forecastDataList:
 
    tempList = foreData.columns[foreData.columns.isin(sizeList)].tolist()+foreData.columns[foreData.columns.isin(emoList)].tolist()
    print(tempList)
    df1 = foreData[['Date']+['windcode']+tempList]
    
    
    sizeAndEmoData = pd.merge(sizeAndEmoData,df1,how = 'outer',on = ['Date','windcode'])
    print(sizeAndEmoData)
    sizeAndEmoData.to_hdf('dataFile/data/sizeAndEmoData.h5','df',complevel=9, complib='blosc')
    
    
    
earningData = pd.DataFrame()
earningData = fundamental
for foreData in forecastDataList:
 
    tempList = foreData.columns[foreData.columns.isin(earningList)].tolist()
    print(tempList)
    df1 = foreData[['Date']+['windcode']+tempList]
    
    
    earningData = pd.merge(earningData,df1,how = 'outer',on = ['Date','windcode'])
    print(earningData)
    earningData.to_hdf('dataFile/data/earningData.h5','df',complevel=9, complib='blosc')