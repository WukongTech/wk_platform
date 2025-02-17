# -*- coding: utf-8 -*-
"""
Created on Tue Aug 15 09:00:20 2017

@author: cxd

1. 回归法筛选有效单因子



"""

  #执行浮点除法
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



"""
回归法检验单因子有效性代码
"""
class SingleFactorAnalyze():
    
    def __init__(self):
        self.__datVal = pd.DataFrame()
    
    
    
    
    def MulticollinearityTest(self,datebegin,date,datSelect,factorList,pastPeriodN):
        datSelect.index = datSelect['Date']
        datSelect = datSelect[datebegin:date]
       
        datSelect=datSelect.sort_values(by =['Date'], ascending = False)
        datSelect = datSelect.dropna()
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
        n_factor=len(factorList)
        l=0
        corrSum=pd.DataFrame(np.repeat(0,n_factor*n_factor).reshape((n_factor,n_factor)),columns=factorList,index=factorList)
        for name ,group in datPerPeriod:
            print(name)
            if name in datPerPeriod_groups:
                perPeriod = datPerPeriod.get_group(name)
                corrMatrix_temp=perPeriod[factorList].corr()
                corrSum+=corrMatrix_temp
                l+=1
                if l>=pastPeriodN: break
        corrMatrix=corrSum/pastPeriodN
        print(corrMatrix)
        if len(corrMatrix)>1:
        
            dim_matrix=len(corrMatrix)
            corrlist=[]
            for i in range(dim_matrix-1):
                for j in range(i,(dim_matrix)):
                    sign=corrMatrix.iat[i,j]
                    if sign>0.4 and i!=j:
                        corrlist.append([i,j])
            """
            获得所有已经有相关性的因子代号
            """
            unionList = []
            for iList in corrlist:
                unionList = list(set(unionList).union(set(iList)))
           
            """
            获取和其他因子都没有相关性的因子代号
            """
            others = list(set(range(dim_matrix))^set(unionList))
            
            
            if len(corrlist)==0:
                return list([i] for i in corrMatrix.columns)
            else:
                corr_groups=[]
                corr_group=corrlist.pop(0)
               
                corr_groups.append(corr_group)
               
                N=len(corr_groups)
                
                for x in corrlist:
                    k=0
                    sign2 = 0
                    while k<N:
                    
                        if x[0] in corr_groups[k]:
                         
                           
                           corr_groups[k].append(x[1])
                           corr_groups[k]  =list(set(corr_groups[k]))
                           sign2 = 1
                        
                        k = k+1
                           
                           
                    if sign2 == 0:
                         corr_groups.append([x[0],x[1]])
                         N = N+1
                           
            resultList = corr_groups
            for i in others:
                iList = [i]
                resultList.append(iList)
                
        elif len(corrMatrix)==1:
               resultList = [[0]]
             
        elif len(corrMatrix) == 0:
            
               resultList = factorList
        
        return [[factorList[y] for y in x] for x in resultList]
    
    
    
    
    def MulticollinearityProcess(self,date,recordData, collinear_groups):
        recordData.index = recordData['Date']
        factorList = []
        for value in collinear_groups:
            if len(value)>1:
                values = recordData.loc[date,value]
              
                print(values)
               
                f = values.index[values == max(values)]
             
                factorList.append([f[0]])
              
                print(factorList)
            else:
                factorList.append(value[0])
      
        return factorList
    
    
    
    """
    读取价格和财务指标数据，预处理完的数据
    实际应该增加预处理代码
    """
   
    
    """
    读取第一批因子数据
    """
    def readData2(self):
        print(("data begin load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        self.__datVal  = pd.read_hdf("D://wk_bkt_public/platformData/priceAndFactorData/dataForRegressionPost.h5")
       
        print(("data after load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
    """
    读取第二批因子数据
    """
        
    def readData3(self):
        print(("data begin load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        self.__datVal  = pd.read_hdf("D://wk_bkt_public/platformData/Factor57Data/dataForRegressionNew.h5")
      
        print(("data after load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
   
    
    
    
    def readDataForecast(self,dataName):
        
        print(("data begin load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        
        self.__datVal  = pd.read_hdf("dataFile/data/%s.h5" %(dataName))
        
        print(("data after load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        self.__datVal.to_csv("dataFile/data/earningData.csv",encoding = 'gbk',index = False)
    



    
    
    """
    对第一批因子进行选取列，改列名和排序，另外如果需要做倒数、相反数、对数等处理也可在此函数中完成
    """
    
    
    def __selectDataColumns2(self):
       
        datSelect = self.__datVal[['Date','windcode','Open','Close','industry_name','Volume','st','MV','PE','PB','PS','Turn','free_turn','netProfit_TTM','revTTM','profitGrowthTTM','revenueGrowthTTM','revenueBetweenTTM','profitBetweenTTM','netAssets','PEG','ROE','logreturn1Month','logreturn3Month','logreturn6Month','logreturn12Month','future_return', 'future_logreturn','returnStdOneMonth', 'meanTurnOneMonth']]
        datSelect.rename(columns={'Volume':'volume','PB':'BP','PE':'EP','PS':'SP'}, inplace=True)
     
        '''
        取2002年以后的数据进行分析，与分层回测保持一致
        '''
        datSelect = datSelect[datSelect['Date']>='20020101']
      
        datSelect['MV_rev'] = 1/datSelect['MV']
        #datSelect['meanTurnOneMonth'] = 1/datSelect['meanTurnOneMonth']
        #datSelect['returnStdOneMonth'] = 1/datSelect['returnStdOneMonth']
        #datSelect['logreturn1Month_rev'] = -datSelect['logreturn1Month']
        datSelect = datSelect.sort_values(by =['windcode','Date'], ascending = True)
        
        return datSelect
    
    
    
    
    
    
    """
    对第二批因子进行选取列，改列名和排序，另外如果需要做倒数、相反数、对数等处理也可在此函数中完成
    """
    
    def __selectDataColumns3(self):
        
        
        datSelect = self.__datVal.rename(columns = {"index1":"netProfit_TTM", "index2":"revGrowth_TTM","index3":"netProfitGrowth_TTM","index4":"PB","index5":"PE_TTM","index6":"PEG_TTM","index7":"fcffToMV_TTM","index8":"grossProfitMargin_TTM","index9":"netProfitMargin_TTM","index10":"totalAssetTurnover_TTM","index11":"periodCostRate_TTM","index12":"currentRatio_TTM","index13":"ROE_TTM","index14":"cashToRevenue_TTM","index15":"mainBusinessRate_TTM","index16":"debtToAssets_TTM", "index17":"equityMultiplier_TTM","index18":"PS_TTM","index19":"cashToProfit_TTM","index20":"ROA_TTM","index21":"cashGrowth_TTM","index22":"ROEGrowth_TTM","index23":"assetToEquity_TTM","index24":"ROE_season","index25":"ROA_season","index26":"grossProfitMargin_season","index27":"netProfitMargin_season","index28":"AssetTurnover_season", "index29":"cashToProfit_season", "index30":"revGrowth_season", "index31":"netProfitGrowth_season", "index32":"cashGrowth_season", "index33":"ROEGrowth_season","index34":"incomeTaxRate","index35":"quickRatio","index36":"cashToEarnings","index37":"capitalReturn","index38":"interestCover","index39":"LTdebtToWorkCapital","index40":"depositReceivedToRev","index41":"equityRatio","index42":"shareHoldersEqRatio","index43":"SHEqToAsset","index44":"proportionOfFixIncome","index45":"LIQDR","index46":"proportionOfMobIncome","index47":"EquityToDebt","index48":"CashRatio","index49":"debtToTanAssets","index50":"invenTurnoverRatio","index51":"invenToSales","index52":"recevTurnoverRatio","index53":"mobAssetTurnoverRatio","index54":"fixAssetTurnoverRatio","index55":"EBITDAEV","index56":"EBITEV","index57":"OPPToMV", "Volume":"volume"})
        #datSelect = self.__datVal[['Date','windcode','Close','industry_name','Volume','st','MV','PEG_TTM','FCFP','grossProfitMargin_TTM','netProfitMargin_TTM','totalAssetTurnover','revenueGrowth_TTM','netProfit_TTM','profitGrowth_TTM','periodCostRate','currentRatio_TTM','ROE_TTM','OCFToRevenue_TTM','cashToRevenue_TTM','debtToAssets_TTM', 'equityMultiplier_TTM','future_logreturn']]
        print(datSelect.columns)
        #datSelect.rename(columns={'Volume':'volume'}, inplace=True)
        '''
        取2002年以后的数据进行分析，与分层回测保持一致
        '''
        datSelect = datSelect[datSelect['Date']>='20020101']
        '''
        datSelect['EP'] = 1/datSelect['PE']
        datSelect['BP'] = 1/datSelect['PB']
        datSelect['SP'] = 1/datSelect['PS']
        datSelect['MV_rev'] = 1/datSelect['MV']
        datSelect['meanTurnOneMonth'] = 1/datSelect['meanTurnOneMonth']
        datSelect['returnStdOneMonth'] = 1/datSelect['returnStdOneMonth']
        datSelect['logreturn1Month_rev'] = -datSelect['logreturn1Month']
        '''
        datSelect = datSelect.sort_values(by =['windcode','Date'], ascending = True)
        
        return datSelect   
        
    
    
    
    def __selectDataColumnsForecastData(self):
        
        print(self.__datVal.columns)
        datSelect = self.__datVal.rename(columns = {"stock_code":"windcode", "con_date":"Date"})
        """
        为下面的合并做一些数据格式的处理
        """
        datSelect['Date'] = pd.to_datetime(datSelect['Date'],format = '%Y-%m-%d')
        datSelect['windcode'] = datSelect['windcode'].astype('str')
        
        
        
        
        """
        将原来的数据中的windcode去掉后面的SZ和SH，和forecast数据合并
        """
        def removeLast(obs):
            obs = str(obs)
            return obs[:6]
        
        
        datForMerge = pd.read_hdf("D://wk_bkt_public/platformData/priceAndFactorData/dataForRegressionPost.h5")
        datForMerge = datForMerge[['Date','windcode','Volume','future_logreturn','MV','industry_name']]
        datForMerge.rename(columns={'Volume':'volume'}, inplace=True)
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
    
    
    """
    主要用于周频测试，得到未来一周的股票收益率
    """
  
    
    
    def __getWeeklyData(self, datSelect,beginDate, endDate):
        
        
        MondayList = []
        m = datetime.datetime.strptime(beginDate,'%Y%m%d')
        
        delta = datetime.timedelta(days=7)
        while m<=datetime.datetime.strptime(endDate,'%Y%m%d'):
            MondayList.append(m)
            m=m+delta
            
        
        datSelect['Date'] =pd.to_datetime(datSelect['Date'], format = '%Y-%m-%d')
        DateList = datSelect['Date']
        
        changeDate = []
        for monday in MondayList:
            for date in DateList:
             
                date = datetime.datetime(date.year, date.month, date.day)
                if date>=monday:
                    
                    changeDate.append(date)
                    break
      
        changeDate = list(set(changeDate))

        datSelect = datSelect[datSelect['Date'].isin(changeDate)]
    

        
        datFinal = pd.DataFrame()
        
        group = datSelect.groupby('windcode')
        groups = datSelect.groupby("windcode").groups
         
     
      

        for inst in list(groups.keys()):
        
       
            datNew = group.get_group(str(inst)).sort_values(by = 'Date')
            
            
            datNew['future_logreturn_weekly'] = np.log((datNew.shift(-1))['Close']/datNew['Close'])
            
            datFinal = datFinal.append(datNew)
       
        
        return datFinal
    
    
    
    
    
        
    """
    下面两个函数是选取牛熊震荡市时间段，以及最近一年数据的
    """
    
    """
    注意这里的数据Date列的数据结构是什么，如果是int，在筛选日期的时候要用对应的int类型的begindate和enddate
    """
    def __timeLimit(self, datSelect, begindate, enddate):
        if(type(datSelect.loc[datSelect.index[0],'Date'])==np.int64):
            
            datSelect = datSelect[(datSelect['Date'].values>=int(begindate)) & (datSelect['Date'].values<=int(enddate))]
        else:
            
            datSelect = datSelect[(datSelect['Date']>= str(begindate)) & (datSelect['Date']<=str(enddate))]
        
        return datSelect
        
    

    
    
    
    
    
    """
    下面三个是按照不同的截面来计算调仓时间之间的股票收益率的函数，一般用getmonlydata2
    """
    
    

    """
    按月截面,使用resample函数
    """
    def __getMonthlyData1(self, datSelect):
        """
        按照月份为单位resample
        """
        datSelect['Date'] =pd.to_datetime(datSelect['Date'], format = '%Y%m%d')
        datSelect.set_index('Date', inplace=True)
        print('before resample')
        datSelect = datSelect.groupby('windcode').resample('MS',fill_method ='ffill')   
        datSelect.index = datSelect.index.droplevel()
        datSelect = datSelect.reset_index()
        print('after resample')
        return datSelect
        
    """
    按月截面，使用每月第一个交易日数据
    """
    def __getMonthlyData2(self, datSelect):
        
        
        datNew = pd.DataFrame()
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
                       
        currentMonth = 0
        for name, group in datPerPeriod:
            if name in datPerPeriod_groups:
                perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                perPeriod['Date'] =pd.to_datetime(perPeriod['Date'], format = '%Y-%m-%d')
                date = pd.DatetimeIndex(perPeriod['Date'])
                
                if date.month[0] == currentMonth:
                    continue
                else:
                    datNew = datNew.append(perPeriod)
                    currentMonth = date.month[0] 
        
        return datNew
                                                                  
        
       
    
    """
    按月截面，按照20个交易日间隔截面
    """
    def __getMonthlyData3(self, datSelect):
        datNew = pd.DataFrame()
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
                       
        tradeDayCount = 0
        for name, group in datPerPeriod:
            if name in datPerPeriod_groups:
                perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                
                if tradeDayCount%20 != 1:
                    tradeDayCount +=1
                    continue
                else:
                    datNew = datNew.append(perPeriod)
                    tradeDayCount +=1
        
        return datNew
    
    
    
    
    """
    给数据按照因子值分组，主要用于分析因子对不同ROE和MV的适应性
    这里都是分成三组的情形，其他的分组数目需要调整代码
    """
    def __divide_data(self, datSelect,col):
        print(col)
        print(col+'_level')
        datSelect[col+'_level'] = 0

        
        x1=float(datSelect[col].quantile(0.33))
        x2=float(datSelect[col].quantile(0.67))
    
        datSelect.loc[datSelect[col].values<x1, col+'_level'] = 1
        datSelect.loc[(datSelect[col].values>=x1) & (datSelect[col].values<x2), col+'_level'] = 2
        
        datSelect.loc[datSelect[col].values>=x2, col+'_level'] = 3
        datSelect = datSelect[datSelect[col+'_level'] !=0]
        datSelect = datSelect.sort_values(by =['windcode','Date'], ascending = True)
      
        return datSelect
    
    
    
    
    
    
    """
    数据预处理函数，哪些预处理在数据库那边处理，哪些在模型里处理，需要确定
    1. 缺失值处理
    2. 极值处理
    3. 每期股票的预期收益率（月收益率）
    """
    def __dataPreProcess(self, perPeriod, factorName,freq):
        """
        剔除掉成交量为0的交易日数据,删除停牌股票
        """
  
        perPeriod = perPeriod[perPeriod['volume'] != 0]
        if freq == 'month':
            if(factorName=='MV'):
                
                perPeriod = perPeriod[['windcode',factorName,'industry_name', 'future_logreturn']]
            else:
                perPeriod = perPeriod[['windcode',factorName, 'MV','industry_name', 'future_logreturn']]
           
        elif freq == 'week':
            if(factorName=='MV'):
                
                perPeriod = perPeriod[['windcode',factorName,'industry_name', 'future_logreturn_weekly']]
            else:
                perPeriod = perPeriod[['windcode',factorName, 'MV','industry_name', 'future_logreturn_weekly']]
           
            
        #print perPeriod  
        perPeriod = perPeriod.dropna()   #此处dropna不能缺少，否则会导致回归失败
        
 
        
        if len(perPeriod) < 30:   #原始输入数据存在几个数据量较小的月份数据，剔除
              return (False, perPeriod)
                        
        """
        因子中位数去极值
        """
        def replaceFunc(datIndustry):
            datIndustry[factorName] = datIndustry[factorName].astype('float')
            medianValue = datIndustry[factorName].median()
            medianAbs = abs(datIndustry[factorName] - medianValue).median()
            datIndustry.ix[datIndustry[factorName] > medianValue+3*medianAbs, factorName] = medianValue+3*medianAbs
            datIndustry.ix[datIndustry[factorName] < medianValue-3*medianAbs, factorName] = medianValue-3*medianAbs
            datIndustry[factorName] = (datIndustry[factorName] - datIndustry[factorName].mean())/datIndustry[factorName].std()
            return datIndustry
        perPeriod = perPeriod.groupby('industry_name').apply(replaceFunc)
        
        
        
       
        """
        因子标准化，标准化方式不同得出来的因子收益率相差较大
        """
        perPeriod[factorName] = (perPeriod[factorName] - perPeriod[factorName].mean())/perPeriod[factorName].std()
            
    
        perPeriod = perPeriod.fillna(0)

        return (True, perPeriod)
        
        
    """
    OLS回归
    """
    def __OlSRegression(self, perPeriod, factorName):
         logreturnArray = perPeriod['future_logreturn'].values  
         factorArray = perPeriod[factorName].values
                    
         dummy = sm.categorical(perPeriod['industry_name'].values, drop = True)
         factorArray = np.column_stack((factorArray, dummy))  
                        
                    
         model = sm.OLS(logreturnArray, factorArray)
         #model = smf.ols(formula='future_logreturn ~ %s+C(industry_name)'%(factorName), data=perPeriod)
         results = model.fit()
         
         return results
                        
         """
         print results.summary()
         print results.params
         print results.params[0]
         modelNew = smf.ols(formula='future_logreturn ~ %s+C(industry_name)'%(factorName), data=perPeriod)
         resultsNew = modelNew.fit()
         print resultsNew.summary()
         print resultsNew.params
         print resultsNew.params[0]
         """
    
    """
    WLS回归
    """
    def __WLSRegression(self, perPeriod, factorName, freq):
        """
        方法一：
        以下代码用于解决异方差问题
        OLS的残差平方记为u^2.用log(u^2)对单因子和行业因子做回归，得到log(u^2)的预测值luhat^2。
        用来实现WLS的权重则为w=1/exp(luhat^2)。以w为权重做WLS
        """
                     
        """
        residSquare = np.square(results.resid) 
        residSquare = np.nan_to_num(residSquare)
        logResidual = np.log(residSquare)
        model2 = sm.OLS(logResidual, BPArray)
        results2 = model2.fit()
                                
        predictValue = results2.fittedvalues
        w = 1/(np.exp(predictValue))
                                
        model3 = sm.WLS(logreturnArray, BPArray, weights = w)
        results3 = model3.fit()
        """
                      
        """
        方法二：WLS使用个股流通市值的平方根作为权重，有利于消除异方差性--->(T值和因子收益率和OLS相差不大)
        """
                        
        if freq == 'month':
            logreturnArray = perPeriod['future_logreturn'].values 
        elif freq == 'week':
            logreturnArray = perPeriod['future_logreturn_weekly'].values 
            
        factorArray = perPeriod[factorName].values
        dummy = sm.categorical(perPeriod['industry_name'].values, drop = True)
        factorArray = np.column_stack((factorArray, dummy))  
        if(factorName=='MV'):
             model3 = sm.OLS(logreturnArray, factorArray)
        else:
             weightArray = np.sqrt(perPeriod['MV'].values)
            
             model3 = sm.WLS(logreturnArray, factorArray, weights = weightArray)
        results3 = model3.fit()
                        
        """
        model3 = smf.ols(formula='future_logreturn ~ %s+C(industry_name)'%(factorName), data=perPeriod,weights = weightArray )
        results3 = model3.fit()
        """
        
        return results3
    
    
    
    """
    分位数回归，目前不用，当时写是为了处理PEG，看会不会有改善
    """
    
    def __QTLRegression(self, perPeriod, factorName):
        
        model3 = smf.quantreg(formula='future_logreturn ~ %s+C(industry_name)'%(factorName), data=perPeriod)
        results3 = model3.fit(q=.1)
        #print results3.summary()
                        
     
        
        return results3
    
    
    
    
    
    
    def __WLSRegressionInsideIndustry(self, perPeriod, factorName):
        """
        方法一：
        以下代码用于解决异方差问题
        OLS的残差平方记为u^2.用log(u^2)对单因子和行业因子做回归，得到log(u^2)的预测值luhat^2。
        用来实现WLS的权重则为w=1/exp(luhat^2)。以w为权重做WLS
        """
                     
        """
        residSquare = np.square(results.resid) 
        residSquare = np.nan_to_num(residSquare)
        logResidual = np.log(residSquare)
        model2 = sm.OLS(logResidual, BPArray)
        results2 = model2.fit()
                                
        predictValue = results2.fittedvalues
        w = 1/(np.exp(predictValue))
                                
        model3 = sm.WLS(logreturnArray, BPArray, weights = w)
        results3 = model3.fit()
        """
                      
        """
        方法二：WLS使用个股流通市值的平方根作为权重，有利于消除异方差性--->(T值和因子收益率和OLS相差不大)
        """
                        
       
        logreturnArray = perPeriod['future_logreturn'].values  
        factorArray = perPeriod[factorName].values
        dummy=np.ones(len(factorArray))
        #dummy = sm.categorical(perPeriod['industry_name'].values, drop = True)
        factorArray = np.column_stack((factorArray, dummy))  
        if(factorName=='MV'):
             model3 = sm.OLS(logreturnArray, factorArray)
        else:
             weightArray = np.sqrt(perPeriod['MV'].values)
            
             model3 = sm.WLS(logreturnArray, factorArray, weights = weightArray)
        results3 = model3.fit()
                        
        """
        model3 = smf.ols(formula='future_logreturn ~ %s+C(industry_name)'%(factorName), data=perPeriod,weights = weightArray )
        results3 = model3.fit()
        """
        
        return results3
    
    
    
    
    def __getICValue(self, perPeriod, factorName,freq):
         """
         IC值计算，需要排除市值和行业对因子暴露度影响，
         以因子暴露度为因变量，对市值因子和行业因子做回归。
         用残差替代原来的暴露值，求残差与股票预期收益率的相关系数
         """
         if freq == 'month':
             logreturnArray = perPeriod['future_logreturn'].values
         elif freq == 'week':
             logreturnArray = perPeriod['future_logreturn_weekly'].values
         if(factorName=='MV'):
             modelIC = smf.ols(formula='%s ~ C(industry_name)'%(factorName), data=perPeriod)
         else:
             modelIC = smf.ols(formula='%s ~ MV+C(industry_name)'%(factorName), data=perPeriod)
         
         resultsIC = modelIC.fit()
         
         """
         获得残差值
         """
         ICValue = np.corrcoef(resultsIC.resid, logreturnArray)[0][1]
         return ICValue
    




    def __resultplots(self,df1,items,factorName,types,freq):
        df1['Date']=[str(d) for d in df1['Date'].values]
        
        
        datIndex = pd.read_csv("Index/Indexdata.csv", encoding = 'gbk')
        newdf = pd.merge(datIndex, df1, how = 'inner', on = ['Date'])
        newdf.dropna()
    
        x=[datetime.datetime.strptime(d, '%Y-%m-%d').date() for d in newdf['Date']]

        y1=list(newdf[items[0]])
        
        y2=list(newdf[items[1]])
       
        #plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y%m%d'))
        #plt.gca().xaxis.set_major_locator(mdates.DayLocator())
        plt.figure(figsize=(8,24))
        plt.subplot(511)
        """
        周频的用20和5，月频的用30和20
        """
        if freq == 'month':
            
            if types == " ":
                plt.bar(x,y1,width=30, color = "green")
            else:
                plt.bar(x,y1,width=20, color = "green")
            if types == " ":
                plt.plot(x,newdf['logreturn'].cumsum(0)/20-0.01)
            plt.title(factorName + " "+"Factors Return Curve")
        
        elif freq == 'week':
            
            if types == " ":
                plt.bar(x,y1,width=20, color = "green")
            else:
                plt.bar(x,y1,width=5, color = "green")
            if types == " ":
                plt.plot(x,newdf['logreturn'].cumsum(0)/20-0.01)
            plt.title(factorName + " "+"Factors Return Curve")
        
        
        
        plt.subplot(512)
        plt.plot(x,newdf[items[0]].cumsum(0))
        plt.title(factorName + " "+"Factors Cumulative Return")

        plt.subplot(513)
        
        plt.hist(y1,50, normed=1, facecolor='blue', alpha=0.5)
        plt.title(factorName + " "+"Factors Return Histogram")
        
        
        plt.subplot(514)
        
        if freq == 'month':
            
            if types == " ":
                plt.bar(x,y2,width=30, color = "green")
            else:
                plt.bar(x,y2,width=20, color = "green")
            if types == " ":
                plt.plot(x,newdf['logreturn'].cumsum(0)/20-0.01)
            plt.title(factorName + " "+"IC values")
        
        elif freq == 'week':
            
            if types == " ":
                plt.bar(x,y2,width=20, color = "green")
            else:
                plt.bar(x,y2,width=5, color = "green")
            if types == " ":
                plt.plot(x,newdf['logreturn'].cumsum(0)/20-0.01)
            plt.title(factorName + " "+"IC values")
            
            
            
            
        
        plt.subplot(515)
        plt.plot(x,newdf[items[1]].cumsum(0))
        plt.title(factorName + " "+"IC Cumulative values")
        
        
        plt.savefig('Plots/' + factorName + "_" + types+ "_"'AnalysisPlots.jpg')  
        #plt.show()                     
    
    
    
    

       
  
        
        
    
    def data_process(self, begin, end, freq,typeName):
        
        
        
        if typeName == 'fund':
            
            self.readData3() 
            datSelect = self.__selectDataColumns3()
            
            if freq == 'month':
            
                datSelect = self.__getMonthlyData2(datSelect)
            elif freq == 'week':
                datSelect = self.__getWeeklyData(datSelect, begin,end)
                
            datSelect.to_hdf('dataFile/data/dataMonthly/datSelect_'+typeName+'.h5','df',complevel=9, complib='blosc')   
        
        
        elif typeName == 'original':
            self.readData2() 
            datSelect = self.__selectDataColumns2()
            
            if freq == 'month':
            
                datSelect = self.__getMonthlyData2(datSelect)
            elif freq == 'week':
                datSelect = self.__getWeeklyData(datSelect, begin,end)
                
            datSelect.to_hdf('dataFile/data/dataMonthly/datSelect_'+typeName+'.h5','df',complevel=9, complib='blosc')
             
                
        elif typeName == 'forecast':
            dataNameList = ['earningData','growingAndValuationData','sizeAndEmoData']
            for dataName in dataNameList:
                
                self.readDataForecast(dataName)
                datSelect = self.__datVal
                if freq == 'month':
            
                    datSelect = self.__getMonthlyData2(datSelect)
                elif freq == 'week':
                    datSelect = self.__getWeeklyData(datSelect, begin,end)
                 
        
        
                datSelect.to_hdf('dataFile/data/dataMonthly/datSelect_'+typeName+dataName+'.h5','df',complevel=9, complib='blosc')
        
    
    
    
    
    
    def indexdata_process(self,freq,begin,end):
        
        
        self.__index = pd.read_csv("Index/szIndex.csv")
         
        datIndex = self.__index[self.__index['Date']>='2005-04-29']
        datIndex['windcode'] = '000001.SH'
        if freq == 'month':
            datIndex = self.__getMonthlyData2(datIndex)
        elif freq == 'week':
            datIndex = self.__getWeeklyData(datIndex,begin,end)
     
        
        datIndex['logreturn'] = np.log((datIndex['Close']/datIndex.shift(1)['Close']))
        datIndex.to_csv("Index/Indexdata.csv",index=False,encoding = 'gbk')
    
    
    
   
        
        
    
    def __correlation(self, df1,freq):
        
        datIndex = pd.read_csv("Index/Indexdata.csv", encoding = 'gbk')
        print("corraltion between fkt and Index:")
        newdf = pd.merge(datIndex, df1, how = 'inner', on = ['Date'])
        newdf.dropna()
        if freq =='week':
            
            print(np.corrcoef(newdf['future_logreturn_weekly'], newdf["ReturnList"]))
        elif freq =='month':
            print(np.corrcoef(newdf['future_logreturn'], newdf["ReturnList"]))
      



     
    def __resultList(self,df1,count,factorName):
        
            
            IndexFactorList = pd.DataFrame(columns = ['因子名称','T>2期数占比', '|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
            """
            T为正的个数
            """
            count1=0
            for value in df1['Tvalue']:
                if value>2:
                    count1+=1
            
       
         
            """
            获取|T|绝对值序列
            """
            #TvalueListOriginalAbs = np.fabs(TvalueListOriginal)
            TvalueList = df1['Tvalue']
            TvalueListAbs = np.fabs(df1['Tvalue'])
            returnList = df1['ReturnList']
            ICValueList = df1['IC']
            """
            T序列T检验值
            """
            #stat_ols_val, p_ols_val = stats.ttest_1samp(TvalueListOriginal,0)
            stat_val, p_val = stats.ttest_1samp(TvalueList, 0)
         
            """
            WLS
            """
            df2 = pd.DataFrame([[factorName,count1/count, TvalueListAbs.mean(),(TvalueListAbs[(TvalueListAbs > 2)].size)/(TvalueListAbs.size),TvalueList.mean(),TvalueList.mean()/TvalueList.std(),returnList.mean(),stat_val,ICValueList.mean(), ICValueList.std(),ICValueList[(ICValueList > 0)].size / ICValueList.size,ICValueList.mean()/ICValueList.std()]], columns = ['因子名称','T>2期数占比','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
     
            IndexFactorList = IndexFactorList.append(df2)
            
            """
            OLS
            
            df1 = pd.DataFrame([[factorName,count2/count, TvalueListOriginalAbs.mean(),(TvalueListOriginalAbs[(TvalueListOriginalAbs > 2)].size)/(TvalueListOriginalAbs.size),TvalueListOriginal.mean(),TvalueListOriginal.mean()/TvalueListOriginal.std(),returnListOriginal.mean(),stat_ols_val,ICValueList.mean(), ICValueList.std(),ICValueList[(ICValueList > 0)].size / ICValueList.size,ICValueList.mean()/ICValueList.std()]], columns = [u'因子名称',u'T>2期数占比',u'|T|均值', u'|T|>2占比', u'T均值',u'T均值/T标准差',u'因子收益率',u'T序列检验值',u'IC均值',u'IC标准差',u'IC>0比例',u'IR'])
            IndexFactorOLSList = IndexFactorOLSList.append(df1)
            """    
            
            return IndexFactorList
        
        
        
        

    
    def __select_specialYear(self, df1,dateList,factorName):
        datSelect = pd.DataFrame()
        Result = pd.DataFrame()
        
        for begindate,enddate in sorted(list(dateList.items()),key=lambda e:e[0]):
            
            dattemp = self.__timeLimit(df1, begindate, enddate)
          
            Result_temp = self.__resultList(dattemp,len(dattemp),factorName)
            Result_temp['Time'] = begindate + '-' + enddate
            datSelect = datSelect.append(dattemp)
            Result = Result.append(Result_temp)
            
        
        Result_total = self.__resultList(datSelect,len(datSelect),factorName)
        
        Result_total['Time'] = 'total'
        Result = Result.append(Result_total)
        
        return Result

            
            
        
    def __select_recentOneYear(self, datSelect):
        todaydate = datetime.date.today()
        begindate=datetime.datetime((todaydate.year-1),todaydate.month,todaydate.day)
        enddate = todaydate
        
        datSelect = self.__timeLimit(datSelect, begindate, enddate)
        
        return datSelect
    
    
    
    
    
    

    def __singleFactorRegression(self, datSelect, factorList, freq):
        
   
        """
        记录因子各项评价指标
        """
        Return_allfactors = pd.DataFrame()
        
        #IndexFactorOLSList = pd.DataFrame(columns = [u'因子名称',u'T>2期数占比', u'|T|均值', u'|T|>2占比', u'T均值',u'T均值/T标准差',u'因子收益率',u'T序列检验值',u'IC均值',u'IC标准差',u'IC>0比例',u'IR'])
        
        """
        按照每个截面分组回归
        """
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
        """
        遍历所有因子 
        """
        for factorName in factorList:  
            #print factorName
            count = 0
            
            Date = np.array([])
            
            TvalueList = np.array([])
            returnList = np.array([])
            ICValueList = np.array([])
            
            """
            遍历所有时间截面
            """
            for name, group in datPerPeriod:
                #print name
                if name in datPerPeriod_groups:
                    perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                    
                    #print perPeriod
                    
                    (ret, perPeriod) = self.__dataPreProcess(perPeriod, factorName,freq)
                   
                   
                    if ret == False:
                        continue
                    print(perPeriod)
                    
                    results3 = self.__WLSRegression(perPeriod, factorName,freq)
                    #results3 = self.__QTLRegression(perPeriod, factorName)
                    ICValue = self.__getICValue(perPeriod, factorName, freq)
                    
                    
                    
                    Date=np.append(Date,name)  
                    
                    TvalueList = np.append(TvalueList, results3.tvalues[0])
                    returnList = np.append(returnList, results3.params[0])
                    ICValueList = np.append(ICValueList, ICValue)
                    
                    count +=1
          
            ResultList = pd.DataFrame({"Date":Date,"Tvalue":TvalueList,"ReturnList":returnList,"IC":ICValueList})           
            
            
            
            
            """
            画图
            """
            
            
            ResultList.to_csv("dataFile/ResultList.csv",encoding = 'gbk',index=False)
            
            df1=pd.read_csv("dataFile/ResultList.csv", encoding = 'gbk')
            
            
            """
            画全部的图，把所有因子的因子收益率放在同一个表中
            """
            Return_allfactors['Date'] = ResultList['Date']
            Return_allfactors[factorName] = ResultList['ReturnList'].cumsum(0)
            
            Return_allfactors.to_csv("dataFile/Return_allfactors.csv", encoding = 'gbk',index=False)
            
            items=["ReturnList","IC"]
            types=" "
            
            self.__resultplots(df1, items, factorName,types,freq)
            
            #self.__correlation(df1)
            
            
            IndexFactorList = self.__resultList(df1,count,factorName)
            
            
            bullList = {'2005-07-11':'2007-10-16','2008-11-04':'2009-08-04','2014-05-31':'2015-06-12','2015-08-26':'2015-12-22','2016-01-28':'2017-08-28'}
            bull = self.__select_specialYear(df1,bullList,factorName)
            
            bearList = {'2007-10-16':'2008-11-04','2010-11-08':'2012-12-13','2015-06-12':'2015-08-26'}
            bear = self.__select_specialYear(df1,bearList,factorName)
            
            shockList = {'2009-08-04':'2010-11-08','2012-12-03':'2014-05-31'} 
           
            
            shock = self.__select_specialYear(df1,shockList,factorName)
            
            """
            最近一年
            """
            
            recent1yr = self.__select_recentOneYear(df1)
            
            items=["ReturnList","IC"]
            types="recent_1yr "
            
            self.__resultplots(recent1yr, items, factorName,types,freq)
            
            recent1yrResult = self.__resultList(recent1yr,len(recent1yr),factorName)
           
            """
            写到一个表中
            """
            
            writer = pd.ExcelWriter('dataFile/singleFactor/singleFactorOutput_'+factorName+'.xlsx', options = {'encoding' : 'utf-8'}, engine = 'xlsxwriter')
       
            IndexFactorList.to_excel(writer, '单因子测试',index=False)
            
            bull.to_excel(writer,'牛市',index = False)
            
            bear.to_excel(writer,'熊市',index = False)
            
            shock.to_excel(writer,'震荡市',index = False)
            
            recent1yrResult.to_excel(writer,'最近一年',index = False)
            
            
            """ 
            print count
            print 'OLS result'
            print IndexFactorOLSList
            """
            print('WLS result')
            print(IndexFactorList)
            
            
        
            
        
        
        
    def __singleFactorRegression_Industry(self, datSelect,factorList,freq):
        

      
        """
        记录因子各项评价指标
        """
        IndexFactorList_Industry = pd.DataFrame(columns = ['因子名称','行业','平均样本量','T>2期数占比','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
       
        
        for factorName in factorList: 
            
             
             """
             按照行业分组
             """
             
            
             datPerIndustry = datSelect.groupby("industry_name")
             datPerIndustry_groups = datSelect.groupby("industry_name").groups
             
             
        
             if True:
                 for name_industry, group_industry in datPerIndustry:
                     
                     print(name_industry)
                     
                     if name_industry in datPerIndustry_groups:
                         
                         
                         perIndustry = datPerIndustry.get_group(name_industry)
                         name_industry = name_industry.encode('utf-8')
                       
                             
                         perIndustry = perIndustry.dropna()
                         if len(perIndustry)>18000:
                             
                            
                             
                             """
                             按照每个截面分组回归
                             """
                             datPerPeriod = perIndustry.groupby("Date")
                             datPerPeriod_groups = perIndustry.groupby("Date").groups
                
                            
                   
                      
                             Date = np.array([])
                             SampleAmount = np.array([])
                             
                             TvalueList = np.array([])
                             returnList = np.array([])
                             ICValueList = np.array([])
                    
                             """
                             遍历所有时间截面
                             """
                             for name, group in datPerPeriod:
                                 
                                 if name in datPerPeriod_groups:
                                     
                                     perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                                     
                            
                                     (ret, perPeriod) = self.__dataPreProcess(perPeriod, factorName,freq)
                                     
                                  
                                     if ret == False:
                                        
                                         continue
                        
                                    
                                     results3 = self.__WLSRegressionInsideIndustry(perPeriod, factorName,freq)
                                     #print results3.summary()
                                     ICValue = self.__getICValue(perPeriod, factorName, freq)
                               
                                   
                                     Date=np.append(Date,name)
                                     SampleAmount=np.append(SampleAmount,len(perPeriod))
                                     
                                     TvalueList = np.append(TvalueList, results3.tvalues[0])
                                     returnList = np.append(returnList, results3.params[0])
                                     ICValueList = np.append(ICValueList, ICValue)
                        
                                     #print ICValueList
                    
                             count1=0
                             for value in TvalueList:
                                 if value>2:
                                     count1+=1
                    
                             
                             """
                             获取|T|绝对值序列
                             """
                            
                             TvalueListAbs = np.fabs(TvalueList)
                             """
                             T序列T检验值
                             """
                             
                             stat_val, p_val = stats.ttest_1samp(TvalueList, 0)
                 
                             """
                             WLS
                             """
                             
                             df1 = pd.DataFrame([[factorName,name_industry,SampleAmount.mean(),count1/len(TvalueList),TvalueListAbs.mean(),(TvalueListAbs[(TvalueListAbs > 2)].size)/(TvalueListAbs.size),TvalueList.mean(),TvalueList.mean()/TvalueList.std(),returnList.mean(),stat_val,ICValueList.mean(), ICValueList.std(),ICValueList[(ICValueList > 0)].size / ICValueList.size,ICValueList.mean()/ICValueList.std()]], columns = ['因子名称','行业','平均样本量','T>2期数占比','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
                          
                             IndexFactorList_Industry = IndexFactorList_Industry.append(df1)
               
                            
                
        print('WLS result')
        print(IndexFactorList_Industry)
                
                         
        #IndexFactorList_Industry.to_csv("dataFile/SingleFactorTest_Industry.csv", encoding = 'gbk',index=False)
        
        return IndexFactorList_Industry
        
        
        
     
        
        
    def __singleFactorRegression_classification(self, datSelect,factorList,item,freq):
        
        if item =='cycle':
            
            datSelect['cycle'] = '非周期'
            #print set(datSelect['industry_name'])  
            cyclelist = ['钢铁(中信)','基础化工(中信)','汽车(中信)','机械(中信)','有色金属(中信)','交通运输(中信)','建筑(中信)','建材(中信)']
            datSelect.loc[datSelect['industry_name'].isin(cyclelist), 'cycle']='周期'
           
            
        else:
            datSelect = self.__divide_data(datSelect,item)
   
        
     
    
        """
        记录因子各项评价指标
        """
        IndexFactorList = pd.DataFrame(columns = ['因子名称',str(item),'平均样本量','T>2期数占比','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
        #IndexFactorOLSList = pd.DataFrame(columns = [u'因子名称',u'周期',u'样本量',u'|T|均值', u'|T|>2占比', u'T均值',u'T均值/T标准差',u'因子收益率',u'T序列检验值',u'IC均值',u'IC标准差',u'IC>0比例',u'IR'])
        
        for factorName in factorList: 
            
       
             """
             按照行业分组
             """
             
             if item =='cycle':
                 datPerCycle = datSelect.groupby(item)
                 datPerCycle_groups = datSelect.groupby(item).groups
             
             else:
                 datPerCycle = datSelect.groupby(item+"_level")
                 datPerCycle_groups = datSelect.groupby(item+"_level").groups
                
                 
             
        
             if True:
                 for name_cycle, group_cycle in datPerCycle:
                     
                       
                     
                     
                     if name_cycle in datPerCycle_groups:
                         
                         #print name_cycle
                         perCycle = datPerCycle.get_group(name_cycle)
                         #name_cycle = name_cycle.encode('utf-8')
                         
                         
                         if len(perCycle)>3000:
                             
                         
                             """
                             按照每个截面分组回归
                             """
                             datPerPeriod = perCycle.groupby("Date")
                             datPerPeriod_groups = perCycle.groupby("Date").groups
                
                             
                   
                    
                             Date = np.array([])
                             SampleAmount = np.array([])
                             
                             TvalueList = np.array([])
                             returnList = np.array([])
                             ICValueList = np.array([])
                    
                             """
                             遍历所有时间截面
                             """
                             for name, group in datPerPeriod:
                                 
                                 if name in datPerPeriod_groups:
                                     
                                     perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                                     
                            
                                     (ret, perPeriod) = self.__dataPreProcess(perPeriod, factorName, freq)
                                     
                                     
                                     if ret == False:
                                         continue
                        
                                   
                                     results3 = self.__WLSRegression(perPeriod, factorName,freq)
                                     #print results3.summary()
                                     ICValue = self.__getICValue(perPeriod, factorName,freq)
                                    
                            
                                     Date=np.append(Date,name)
                                     SampleAmount=np.append(SampleAmount,len(perPeriod))
                                     
                                     TvalueList = np.append(TvalueList, results3.tvalues[0])
                                     returnList = np.append(returnList, results3.params[0])
                                     ICValueList = np.append(ICValueList, ICValue)
                        
                                     #print ICValueList
                    
                             count1=0
                             for value in TvalueList:
                                 if value>2:
                                     count1+=1
                    
                             """
                             获取|T|绝对值序列
                             """
                            
                             TvalueListAbs = np.fabs(TvalueList)
                             """
                             T序列T检验值
                             """
                             
                             stat_val, p_val = stats.ttest_1samp(TvalueList, 0)
                 
                             """
                             WLS
                             """
                             
                             df1 = pd.DataFrame([[factorName,name_cycle,SampleAmount.mean(),count1/len(TvalueList),TvalueListAbs.mean(),(TvalueListAbs[(TvalueListAbs > 2)].size)/(TvalueListAbs.size),TvalueList.mean(),TvalueList.mean()/TvalueList.std(),returnList.mean(),stat_val,ICValueList.mean(), ICValueList.std(),ICValueList[(ICValueList > 0)].size / ICValueList.size,ICValueList.mean()/ICValueList.std()]], columns = ['因子名称',str(item),'平均样本量','T>2期数占比','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
                             #print df1
                             IndexFactorList = IndexFactorList.append(df1)
                    
                            
               
                            
                
        print('WLS result')
        print(IndexFactorList)
                
                         
        #IndexFactorList_cycle.to_csv("SingleFactorTest_ROE.csv", encoding = 'gbk',index=False)
           
        return IndexFactorList
        
        
        
        
    
    
    
    
    
    def output(self,datSelect, factorList,freq):
        
        
        """
        单因子测试，每个因子输出一个文件
        """
        self.__singleFactorRegression(datSelect,factorList,freq)
        '''
        """
        周期/非周期性行业
        """
        self.__singleFactorRegression_classification(datSelect,factorList,'cycle',freq).to_csv('dataFile/singleFactor/singleFactorOutput_cycle.csv',encoding = 'gbk', index=False)
        
        
        
        """
        不同层次的ROE和市值。
        要执行这两步的话，需要在表格数据里有该列名，'ROE','MV',输出文件名也要改，前后统一
        """
        self.__singleFactorRegression_classification(datSelect,factorList,'ROE_TTM',freq).to_csv('dataFile/singleFactor/singleFactorOutput_ROEclass.csv',encoding = 'gbk', index=False)
        self.__singleFactorRegression_classification(datSelect,factorList,'MV',freq).to_csv('dataFile/singleFactor/singleFactorOutput_MVclass.csv',encoding = 'gbk', index=False)
        
        
        """
        在不同行业内执行单因子测试，这部分意义不大
        """
        self.__singleFactorRegression_Industry(datSelect,factorList).to_csv('dataFile/singleFactor/singleFactorOutput_Industry.csv',encoding = 'gbk', index=False)
        '''
       
        
        
    def getFactorRecord(self, datSelect,factorList,freq):
        
        
        factorRecord = pd.DataFrame(columns=['Date']+factorList)
        """
        按照每个截面分组回归
        """
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
        """
        遍历所有因子 
        """
        for factorName in factorList:  
            print(factorName)
            
            count = 0
            
            Date = np.array([])
            
            TvalueList = np.array([])
            returnList = np.array([])
            ICValueList = np.array([])
            
            """
            遍历所有时间截面
            """
            for name, group in datPerPeriod:
                #print name
          
                if name in datPerPeriod_groups:
                    perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                    
                    #print perPeriod
                    
                    (ret, perPeriod) = self.__dataPreProcess(perPeriod, factorName,freq)
      
                 
                   
                    if ret == False:
                        continue
                   
                    
                    results3 = self.__WLSRegression(perPeriod, factorName,freq)
                    #results3 = self.__QTLRegression(perPeriod, factorName)
                    ICValue = self.__getICValue(perPeriod, factorName, freq)
                 
                    
                    
                    Date=np.append(Date,name)  
                    
                    TvalueList = np.append(TvalueList, results3.tvalues[0])
                    returnList = np.append(returnList, results3.params[0])
                    ICValueList = np.append(ICValueList, ICValue)
                    
                    count +=1
          
            ResultList = pd.DataFrame({"Date":Date,"Tvalue":TvalueList,"ReturnList":returnList,"IC":ICValueList})           
           
        
            ResultList.to_csv("dataFile/tempFile/RecordDataframe/RecordDataframe_"+factorName+".csv",encoding = 'gbk',index = False)
        
        
        
        
    def chooseValidFactors(self, datSelect,factorList, freq, periodN):
        
        vaildFactors = pd.DataFrame(columns = ['Date']+factorList)
        
        vaildFactors['Date'] = sorted(list(set(datSelect['Date'])))
        vaildFactors.index = vaildFactors['Date']
        del vaildFactors['Date']
        

            
        
        print("begin select factors")
            
        for factorName in factorList:
            print(factorName)
            ResultList = pd.read_csv("dataFile/tempFile/RecordDataframe/RecordDataframe_"+factorName+".csv",encoding = 'gbk')
            
            for i in range(len(ResultList)):
                
                date = ResultList.loc[i,'Date']
                
                #print date
            
                if i>=periodN:
                    datebegin = ResultList.loc[(i-periodN),'Date']
                    ResultList_temp = ResultList[(i-periodN):i]
                    count = periodN
                    factorTemp = self.__resultList(ResultList_temp,count,factorName)
                   
                   
                    """
           
                    |T均值/T标准差|： >0.08
                    因子收益率均值：>0.001
                    abs(T)序列检验：>2
                    IC均值：>0.01
                    #IC>0比例：>0.55
                    |T|>2占比>0.55
                    |IR|>0.25
                    """
                    
                    #abs(factorTemp.loc[0, u'T均值'])>0.25 and factorTemp.loc[0,u'IC>0比例']>0.55 and
                    if abs(factorTemp.loc[0,'T均值/T标准差'])>0.08 and abs(factorTemp.loc[0, '因子收益率'])>0.001 and abs(factorTemp.loc[0, 'IC均值'])>0.01 and abs(factorTemp.loc[0,'T序列检验值'])>2 and factorTemp.loc[0, '|T|>2占比']>0.5: #and abs(factorTemp.loc[0, u'IR'])>0.25:
                        vaildFactors.loc[date,factorName] = 1
                       
                    else:
                        vaildFactors.loc[date,factorName] = 0
                        
        return vaildFactors
        
        """
        每个factorList按照大类因子输入就可以了
        """
        
        '''
        vaildFactors = vaildFactors.reset_index()

        vaildFactorsNew = pd.DataFrame(columns = vaildFactors.columns)
        vaildFactorsNew['Date'] = vaildFactors['Date']
        
        for i in range(len(vaildFactors)):

            if i>=periodN:
                date = vaildFactors.loc[i,'Date']
          
                datebegin = vaildFactors.loc[(i-periodN),'Date']
                factorList = vaildFactors.columns[vaildFactors.ix[i]==1]
                
                print factorList
                if len(factorList)>1:
                    collnear_groups = self.MulticollinearityTest(datebegin,date,datSelect,factorList,periodN)
            
                    print collnear_groups
                    factorListNew  =self.MulticollinearityProcess(date,factorRecord, collnear_groups)
                    print factorListNew
                    vaildFactorsNew.loc[i,vaildFactors.columns] = 0
                    vaildFactorsNew.loc[i,'Date'] = date
                    
                    vaildFactorsNew.loc[i,factorListNew[0]] = 1
              
                else:
                    
                    vaildFactorsNew.loc[i,vaildFactors.columns] = 0
                    vaildFactorsNew.loc[i,'Date'] = date
            
        vaildFactorsNew.to_csv("dataFile/validFactors/valid_factors-third/NewvaildFactors_"+str(periodN)+"_class"+str(classlabel)+".csv",encoding = 'gbk',index = False)      
        '''
        
        
        
        
def main():
    
    
    def execution(factors, periodList,typeName,classlabel=0):
            classlabel = classlabel
            for factorList in factors:
                
                """
                首先获取所有因子全周期每期的因子收益率列表
                """
                SingleFactor.getFactorRecord(datSelect,factorList,freq)
                classlabel = classlabel+1

                for periodN in periodList:
                
                    print(periodN)
                    
                    vaildFactors = SingleFactor.chooseValidFactors(datSelect,factorList, freq, periodN) 
                    vaildFactors.to_csv("dataFile/validFactors/valid_factors-"+typeName+"/vaildFactors_"+str(periodN)+"_"+typeName+"_class"+str(classlabel)+"c2.csv",encoding = 'gbk')
                    """
                    获取加总的期数
                    """
                    vaildFactorsStats = vaildFactors.apply(np.sum,axis = 0)
                    vaildFactorsStats = vaildFactorsStats.T
                  
                    vaildFactorsStats.to_csv("dataFile/validFactors/summary/VFstats_"+str(periodN)+"_"+typeName+"_class"+str(classlabel)+"c2.csv",encoding = 'gbk')
                      
    
    
    
    
    SingleFactor = SingleFactorAnalyze()
    
    """""""""""""""""""""""""""""""""""""""""""""""""""""
    数据整理部分
    """""""""""""""""""""""""""""""""""""""""""""""""""""
    

    begin = '20050601'
    end = '20170901'
    freq = 'month'
    
    typeName = 'forecast'
    
    periodList = [12,24,36]
        
    """
    记录整理好数据，避免多次读取和整理大文件
    """

    #SingleFactor.data_process(begin, end, freq, typeName)
    
    
    """
    获取指数数据
    
    SingleFactor.indexdata_process(freq,begin,end)
    """
    
    
    """""""""""""""""""""""""""""""""""""""""""""""""""""
    执行单因子部分
    """""""""""""""""""""""""""""""""""""""""""""""""""""
    """
    读取刚才保存的数据，如果前面已经执行完毕，可以直接从这一步开始
    """

    """
    输入因子列名
    """
    
    if typeName == 'original':
        print("begin read data")
        datSelect = pd.read_hdf('dataFile/data/dataMonthly/datSelect_'+typeName+'.h5')
        print("end read data")
        print(datSelect.columns)
        factorList = ['MV','logreturn1Month','meanTurnOneMonth','returnStdOneMonth','logreturn3Month','logreturn6Month','logreturn12Month']
        """
        如果要分大类因子的话在这里，拆分成几个factorList
        """
        factors = [factorList]
        execution(factors, periodList,typeName)
        
        
    
    
    elif typeName == 'fund':
        print("begin read data")
        datSelect = pd.read_hdf('dataFile/data/dataMonthly/datSelect_'+typeName+'.h5')
        print("end read data")
        print(datSelect.columns)
        """
        删去一些因子之后，要对这个列表进行调整
        """
        factorList = ['netProfit_TTM','revGrowth_TTM','netProfitGrowth_TTM','PB','PE_TTM','PEG_TTM','fcffToMV_TTM','grossProfitMargin_TTM','netProfitMargin_TTM','totalAssetTurnover_TTM','periodCostRate_TTM','currentRatio_TTM','ROE_TTM','cashToRevenue_TTM','mainBusinessRate_TTM','debtToAssets_TTM','equityMultiplier_TTM','PS_TTM','cashToProfit_TTM','ROA_TTM','cashGrowth_TTM','ROE_season','ROEGrowth_TTM','assetToEquity_TTM','ROA_season','grossProfitMargin_season','netProfitMargin_season','AssetTurnover_season','cashToProfit_season','revGrowth_season','netProfitGrowth_season','cashGrowth_season','ROEGrowth_season','incomeTaxRate','quickRatio','cashToEarnings','capitalReturn','interestCover','LTdebtToWorkCapital','depositReceivedToRev','equityRatio','shareHoldersEqRatio','SHEqToAsset','proportionOfFixIncome','LIQDR','proportionOfMobIncome','EquityToDebt','CashRatio','debtToTanAssets','invenTurnoverRatio','invenToSales','recevTurnoverRatio','mobAssetTurnoverRatio','fixAssetTurnoverRatio','EBITDAEV','EBITEV','OPPToMV']
        factors = [factorList]
        execution(factors, periodList,typeName)
    
        
        """
        如果分大类因子的话
        """
    
        #factorList1 = ['currentRatio_TTM','debtToAssets_TTM', 'equityMultiplier_TTM','assetToEquity_TTM']
        #factorList2 = ['revGrowth_TTM','netProfitGrowth_TTM','cashGrowth_TTM','ROEGrowth_TTM','revGrowth_season','netProfitGrowth_season','cashGrowth_season','ROEGrowth_season']
        #factorList3 = ['PB','PE_TTM','PEG_TTM','PS_TTM','fcffToMV_TTM']
        #factorList4 = ['netProfit_TTM','grossProfitMargin_TTM','netProfitMargin_TTM','totalAssetTurnover_TTM','periodCostRate_TTM','ROE_TTM','cashToRevenue_TTM','mainBusinessRate_TTM','cashToProfit_TTM','ROA_TTM','ROE_season','ROA_season','grossProfitMargin_season','netProfitMargin_season','AssetTurnover_season','cashToProfit_season']
        #factors = [factorList1,factorList2,factorList3,factorList4]
    
    
    elif typeName == 'forecast':
        dataNameList = ['earningData','growingAndValuationData','sizeAndEmoData']
        for dataName in dataNameList:
            print(dataName)
            print("begin read data")
            datSelect = pd.read_hdf('dataFile/data/dataMonthly/datSelect_'+typeName+dataName+'.h5')
            print("end read data")
       
            
            if dataName == 'earningData':
                factorList = ['con_npgrate_1w','con_npgrate_4w','con_npgrate_13w','con_npgrate_26w','con_roe_mom','con_eps_roll_mom','con_np_roll','con_np_roll_mom','con_npgrate_4w_rank','con_npgrate_13w_rank','con_npgrate_26w_rank','report_mean_np_w','report_mean_np_m','report_mean_np_q','report_max_np_w','report_max_np_m','report_max_np_q','report_min_np_w','report_min_np_m','report_min_np_q','eps_dev_75d','eps_std_75d','np_std_25d','np_std_75d']
                factors = [factorList]
                execution(factors, periodList,typeName)
            elif dataName == 'growingAndValuationData':
                growingList = ['con_npcgrate_2y','con_npcgrate_2y_roll']
                valuationList = ['con_pb','con_pe','con_peg','con_pb_mom','con_pe_mom','con_peg_mom','con_pe_roll','con_pe_roll_mom','con_pb_roll','con_pb_roll_mom','con_peg_roll','con_peg_roll_mom','con_pb_rank','con_peg_rank','con_pb_roll_rank','con_pe_roll_rank','con_peg_roll_rank','pe_dev_roll_75d','pb_dev_roll_25d','pb_dev_roll_75d']
                factors = [growingList,valuationList]
                execution(factors, periodList,typeName,classlabel = 1)
            elif dataName == 'sizeAndEmoData':
                sizeList = ['con_na','con_na_mom','con_na_roll','con_na_roll_mom','tcap_rank']
                emoList = ['down_num_m','down_num_q','relative_report_num_75d','organ_num_25d','organ_num_75d','report_num_q','author_num_m','author_num_q','organ_num_m','organ_num_q','overweight_num_q','neutral_num_q']

                factors = [sizeList,emoList]
                execution(factors, periodList,typeName,classlabel = 3)
                

        
    """
    大类因子的gather
    """    
    """
    periodN=12
    vaildFactorsGather = pd.DataFrame()
    classlabel = 0
    while classlabel<4:
      classlabel = classlabel+1
      vaildFactorsNew = pd.read_csv("dataFile/validFactors/valid_factors-second/validFactor_CollineariltyRemoved/NewvaildFactors_"+str(periodN)+"_class"+str(classlabel)+".csv",encoding = 'gbk')
      if classlabel == 1:
          vaildFactorsGather = pd.concat([vaildFactorsGather, vaildFactorsNew], axis=1)
      else:
          vaildFactorsGather = pd.concat([vaildFactorsGather, vaildFactorsNew.drop('Date', 1)], axis=1)
    
    vaildFactorsGather.to_csv("dataFile/validFactors/valid_factors-second/validFactor_CollineariltyRemoved/vaildFactorsGather_"+str(periodN)+".csv",encoding = 'gbk',index = False)
    """
    
    
    
    
    
if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    
