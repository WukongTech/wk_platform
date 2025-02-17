# -*- coding: utf-8 -*-
"""
Created on Wed Jan 03 15:33:56 2018

@author: yangr

对每个因子进行回测，看组合因子为什么没有单因子的效果好

"""

  #执行浮点除法
import pandas as pd
import numpy as np
import time
import os
import shutil
import statsmodels.api as sm
import statsmodels.formula.api as smf
import copy
import matlab.engine
from patsy import dmatrices
import cvxopt
import pylab
from cvxopt import solvers, matrix, spmatrix, mul, div
import math
import datetime

    
"""
多因子收益和风险模型
"""
class MultiFactorReturnRiskModel():
    def __init__(self, localOrServer=0):
        
         """
         保存输入的因子值数据
         """
         self.__datVal = pd.DataFrame()
         """
         保存回归得到的每期因子收益率
         """
         self.__record = pd.DataFrame()
         """
         用来判断程序在本机还是服务器执行，数据文件的绝对路径不同
         """
         self.__localOrServer = localOrServer
         
    """
    多重共线性检查代码
    """
    def __MulticollinearityTest(self):
        pass
    
    
    """
    读取数据源
    """
    def readData(self, begin, end, indexBench):
        print(("data begin load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        """
        判断数据文件的绝对路径
        """
        if self.__localOrServer == 0:
            if indexBench == 'ZZ500':
                self.__datVal = pd.read_hdf("D://wk_bkt_public/platformData/validFactorData/validFactorDataIndex_ZZ500.h5")
            elif indexBench == 'HS300':
                self.__datVal = pd.read_hdf("D://wk_bkt_public/platformData/validFactorData/validFactorDataIndex_HS300.h5")
            elif indexBench == 'SZ180':
                self.__datVal = pd.read_hdf("D://wk_bkt_public/platformData/validFactorData/validFactorDataIndex_SZ180.h5")
            elif indexBench == 'ZZ100':
                self.__datVal = pd.read_hdf("D://wk_bkt_public/platformData/validFactorData/validFactorDataIndex_ZZ100.h5")
            elif indexBench == '':
                self.__datVal = pd.read_hdf("D://wk_bkt_public/platformData/factorDataChenxd/validFactorData_ALL.h5")
        
        else:
            if indexBench == 'ZZ500':
                self.__datVal = pd.read_hdf("/home/wk_bkt_public/platformData/validFactorData/validFactorDataIndex_ZZ500.h5")
            elif indexBench == 'HS300':
                self.__datVal = pd.read_hdf("/home/wk_bkt_public/platformData/validFactorData/validFactorDataIndex_HS300.h5")
            elif indexBench == 'SZ180':
                self.__datVal = pd.read_hdf("/home/wk_bkt_public/platformData/validFactorData/validFactorDataIndex_SZ180.h5")
            elif indexBench == 'ZZ100':
                self.__datVal = pd.read_hdf("/home/wk_bkt_public/platformData/validFactorData/validFactorDataIndex_ZZ100.h5")
       
            elif indexBench == '':
                self.__datVal = pd.read_hdf("/home/wk_bkt_public/platformData/factorDataChenxd/validFactorData_ALL.h5")
        
        print(self.__datVal.columns)
        print(("data after load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        print(self.__datVal.shape)
        """
        区间筛选
        """
        self.__datVal = self.__datVal[(self.__datVal['Date'] >= begin)&(self.__datVal['Date'] <= end)]
        """
        对数据做合法性的检验，
        提出当日停牌股票，ST股票，及上市不满半年的股票
        """
        self.__datVal = self.__datVal[self.__datVal['volume'] != 0]
        self.__datVal = self.__datVal[self.__datVal['trade_status'] == 1]
        self.__datVal = self.__datVal[self.__datVal['st'] == 0]
        self.__datVal = self.__datVal[self.__datVal['industry_name'] != '']
        print(self.__datVal.shape)
        
        """
        删除上市不满一年的股票
        """
        self.__datVal['Date'] =pd.to_datetime(self.__datVal['Date'], format = '%Y-%m-%d')
        self.__datVal['listdate'] =pd.to_datetime(self.__datVal['listdate'], format = '%Y-%m-%d')
        self.__datVal = self.__datVal[(self.__datVal['Date'] - self.__datVal['listdate']).dt.days > 180]
        self.__datVal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in self.__datVal['Date']]
        print(("data after preprocess at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        print(self.__datVal.shape)
        
        
        
        '''
        The following part is modified by Xin-Ji Liu on 2017-12-28
        '''
        ################################### modification start ################################### 
        # Modified by Xin-Ji Liu on 20180102
        # save the original weight of each stock in the column 
        
        
      
        if indexBench:
            bench_weight = pd.read_csv('dataFile/IndexWeightsoriginal_'+indexBench+'_monthly.csv')    
            bench_weight.rename(columns={'s_con_windcode':'windcode', 'trade_dt':'Date','i_weight':'bench_weight'}, inplace = True)
            # bench_weight['Date'] = pd.to_datetime(bench_weight['Date'],format = '%Y%m%d')
            bench_weight['Date'] = bench_weight['Date'].astype(str)
            # self.__datVal.to_csv('dataFile/tempFile/before.csv', encoding = 'gbk', index = False)
            self.__datVal = pd.merge(self.__datVal,bench_weight,how = 'left', on = ['windcode','Date'])
            print((len(self.__datVal)))
            self.__datVal.sort_values(by = ['windcode','Date'],inplace = True)
            #self.__datVal['bench_weight'] = self.__datVal['bench_weight'].fillna(method = 'ffill')
            self.__datVal['bench_weight'] = self.__datVal['bench_weight']/100.0
            #self.__datVal.to_csv('dataFile/tempFile/after.csv', encoding = 'gbk', index = False)
        ################################### modification end ###################################           
        
        
        ################## modificagtion start ############################
        ##Ruijing Yang, 2018-01-19
        ##增加悟空板块
        
        wukongPlate = pd.read_hdf("/home/wk_bkt_public/platformData/validFactorData/wukongPlate.h5")
        self.__datVal = pd.merge(self.__datVal, wukongPlate, how = 'inner', on = ['windcode','Date'])
        
        print(self.__datVal.shape)
        ##################### modification end ##############################
        
        return self.__datVal
    
    
    
   
    
        
    """
    选取有用的列
    """
    def __selectDataColumns(self):
        #datSelect = self.__datVal[['Date', 'windcode','Close','sec_name','industry_name','Volume','trade_status','MV','PB','PE','PS','logreturn1Month','returnStdOneMonth','meanTurnOneMonth','future_return','future_logreturn']]
        """
        如果有因子的因子收益率为负，在这里取倒数
        """
        '''
        self.__datVal['PB'] = 1/self.__datVal['PB']
        self.__datVal['PE_TTM'] = 1/self.__datVal['PE_TTM']
        self.__datVal['DPS'] = 1/self.__datVal['DPS']
        
        #self.__datVal['trix'] = 1/self.__datVal['trix']
        self.__datVal['meanTurnOneMonth'] = 1/self.__datVal['meanTurnOneMonth']
        self.__datVal['meanTurn3Month'] = 1/self.__datVal['meanTurn3Month']
        self.__datVal['meanTurn6Month'] = 1/self.__datVal['meanTurn6Month']
        self.__datVal['meanTurn12Month'] = 1/self.__datVal['meanTurn12Month']
        
        
        #self.__datVal['br'] = 1/self.__datVal['br']
        #self.__datVal['amt_avg20'] = 1/self.__datVal['amt_avg20']
        #self.__datVal['vstd20'] = 1/self.__datVal['vstd20']
        #self.__datVal['rsi24'] = 1/self.__datVal['rsi24']
        self.__datVal['periodCostRate_TTM'] = 1/self.__datVal['periodCostRate_TTM']
        """
        如果要做大市值的话，就注释掉下面这行
        """
        self.__datVal['MV'] = 1/self.__datVal['MV']
        
    
        
        self.__datVal['returnStdOneMonth'] = -1 * self.__datVal['returnStdOneMonth']
        self.__datVal['returnStd3Month'] = -1 * self.__datVal['returnStd3Month']
        self.__datVal['returnStd6Month'] = -1 * self.__datVal['returnStd6Month']
        self.__datVal['returnStd12Month'] = -1 * self.__datVal['returnStd12Month']
        '''
        datSelect = self.__datVal.sort_values(by =['windcode','Date'], ascending = True)
        return datSelect
    
    """
    按月截面,使用resample函数
    """
    def __getMonthlyData1(self, datSelect):
        """
        按照月份为单位resample
        """
        datSelect['Date'] =pd.to_datetime(datSelect['Date'], format = '%Y-%m-%d')
        datSelect.set_index('Date', inplace=True)
        print('before resample')
        datSelect = datSelect.groupby('windcode').resample('MS',fill_method ='ffill')   
        datSelect.index = datSelect.index.droplevel()
        datSelect = datSelect.reset_index()
        print('after resample')
        return datSelect
    
    """
    按月截面，使用每月第一个交易日数据，首选该函数
    """
    def __getMonthlyData2(self, datSelect):
        
        datNew = pd.DataFrame()
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
        currentMonth = 0
        datelist = []
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
                    
                    """
                    记录月初日期
                    """
                    ####modified by Ruijing
                    if date.year[0]>2004:
                        datelist.append(date[0])
        """
        记录datelist
        """
        datelistDataframe = pd.DataFrame(datelist)
        datelistDataframe.columns = ['Date']
        datelistDataframe.to_csv("dataFile/tempFile/dateList.csv", encoding='gbk',index=False)
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
    主要用于周频测试，得到未来一周的股票收益率
    """
  
    
    
    def __getWeeklyData(self, datSelect,beginDate, endDate):
        
        
        MondayList = []
        m = datetime.datetime.strptime(beginDate,'%Y%m%d')

        while m.weekday() != 0:
            #delta = datetime.timedelta(days=1)
            m = m + datetime.timedelta(days=1)
        

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
       
        """
        记录datelist
        """
        datelistDataframe = pd.DataFrame(changeDate)
        datelistDataframe.columns = ['Date']
        datelistDataframe.sort_values('Date', inplace = True)
        datelistDataframe.to_csv("dataFile/tempFile/dateList.csv", encoding='gbk',index=False)
        
        datSelect = datSelect[datSelect['Date'].isin(changeDate)]
    
        """
        2018-01-09 Ruijing Yang 这里需要用到close price，但是数据里没有
        目前的选股方法不需要用到future_logReturnWeekly,所以先不计算了
        """
        '''
        datFinal = pd.DataFrame()
        
        group = datSelect.groupby('windcode')
        groups = datSelect.groupby("windcode").groups
         
     
      

        for inst in groups.keys():
        
       
            datNew = group.get_group(str(inst)).sort_values(by = 'Date')
            
            
            datNew['future_logreturn_weekly'] = np.log((datNew.shift(-1))['Close']/datNew['Close'])
            
            datFinal = datFinal.append(datNew)
       '''
        
        return datSelect
    
    
    
    
    
    """
    截取时间区间范围、指定列，月初截面的数据
    """
    def monthlyDataSelect(self, beginDate, endDate, factorList, indexBench):
        """
        截取指定区间的数据
        """
        datSelect = self.readData(beginDate, endDate, indexBench)
        """
        选取指定列
        """
        datSelect = self.__selectDataColumns()
        """
        获取月初截面的数据
        """
        datSelect = self.__getMonthlyData2(datSelect)
        """
        进行统一预处理
        """
        datSelect = self.__dataPreProcessAll(datSelect, factorList)
        
        return datSelect
    
    
    
    
    
    def weeklyDataSelect(self, beginDate, endDate, factorList,indexBench):
        """
        截取指定区间的数据
        """
        datSelect = self.readData(beginDate, endDate, indexBench)
        """
        选取指定列
        """
        datSelect = self.__selectDataColumns()
        """
        获取周截面的数据
        """
        datSelect = self.__getWeeklyData(datSelect,beginDate, endDate)
        """
        进行统一预处理
        """
        datSelect = self.__dataPreProcessAll(datSelect, factorList)
        
        return datSelect
    
    
    
    
    
    def __dataPreProcessAll(self, datSelect, factorList):
        """
        根据日期截面进行回归
        """
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
                                               
        datAfterProcess = pd.DataFrame()              
        for name, group in datPerPeriod:
            if name in datPerPeriod_groups:
                print(name)
                perPeriod = datPerPeriod.get_group(name)          
                """
                进行数据预处理
                """                              
                perPeriod = self.__dataPreProcess(perPeriod, factorList)
                datAfterProcess = datAfterProcess.append(perPeriod)
        
        return datAfterProcess
    
    """
    每期时间截面数据预处理
    """
    def __dataPreProcess(self, perPeriod, factorList):        
        
        """
        剔除掉有Null值的行
        """
        #perPeriod = perPeriod.dropna()   #此处dropna不能缺少，否则会导致回归失败
        perPeriod = perPeriod.fillna(0)
                                     
                                     
        """
        分行业因子中位数去极值
        """
        for factorName in factorList:
            def replaceFunc(datIndustry):
                medianValue = datIndustry[factorName].median()
                medianAbs = abs(datIndustry[factorName] - medianValue).median()
                datIndustry.ix[datIndustry[factorName] > medianValue+3*medianAbs, factorName] = medianValue+3*medianAbs
                datIndustry.ix[datIndustry[factorName] < medianValue-3*medianAbs, factorName] = medianValue-3*medianAbs
                datIndustry[factorName] = (datIndustry[factorName] - datIndustry[factorName].mean())/datIndustry[factorName].std()
                return datIndustry
            perPeriod = perPeriod.groupby('industry_name').apply(replaceFunc)
        
    
        """
        新的因子暴露度序列缺失的地方设为0.视作存在缺失值时我们认为此个股因子值与全市场平均情况相同
        """
        perPeriod = perPeriod.fillna(0)
        """
        再去一次空值
        """
        perPeriod = perPeriod.dropna()
        
        return perPeriod
    
    
    
    
    """
    针对OPPToMV，筛选掉一些行业
    """
 
    def selectIndustry_process(self):
        '''
        beginDate = '20070101'
        endDate = '20170601'
        '''
        indexBench = 'ZZ100'
        '''
        """
        截取指定区间的数据
        """
        datSelect = self.readData(beginDate, endDate, indexBench)
        """
        选取指定列
        """
        datSelect = self.__selectDataColumns()
        """
        获取股票的OPP因子值
        """
        datSelect['OPPValue'] = datSelect['OPPToMV']*datSelect['MV']
        """
        获取月初截面的数据
        """
        datSelect = self.__getMonthlyData2(datSelect)
        
        datSelect.to_csv("dataFile/index/monthlySelectIndus_"+indexBench+".csv", encoding = 'gbk', index = False)
        
        '''
        datSelect = pd.read_csv("dataFile/index/monthlySelectIndus_"+indexBench+".csv", encoding = 'gbk')
        
        OPP_sum = pd.pivot_table(datSelect, index = 'Date', columns = 'industry_name', values = 'OPPValue',aggfunc=[np.sum])
       
        MV_sum = pd.pivot_table(datSelect, index = 'Date', columns = 'industry_name', values = 'MV',aggfunc=[np.sum])
        
        OPPToMV_industry = OPP_sum/MV_sum
        
        OPPToMV_industry.to_csv("dataFile/tempFile/OPPToMV_industry"+indexBench+".csv",encoding = 'gbk')
        
        
        
        
    """""""""
    以下为因子收益率加权方式需要用到的函数
    """""""""
    def __WLSRegressionRoll(self, date,datSelect, factorList, industryList, validFactorList):
        
        """
        用来记录每个截面的因子收益率, Date+factorList+industryList的数据格式
        """
        columnName = ['Date']
        columnName.extend(factorList)
        columnName.extend(industryList)
        recordDataframe = pd.DataFrame(columns =columnName)
        
        """
        用来记录每只股票的残差,列为股票代码windcode
        """
        columnName_resid = ['Date']
        windcodeList = list(set(datSelect['windcode']))
        columnName_resid.extend(windcodeList)
        recordDataframeResid = pd.DataFrame(columns = columnName_resid)
        
        """
        根据日期截面进行回归
        """
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
        #count = 0
        """
        需要保存因子收益率矩阵 以及 因子值矩阵
        """                       
        for name, group in datPerPeriod:
            if name in datPerPeriod_groups:
                #print name
                perPeriod = datPerPeriod.get_group(name)   
                """
                按照windcode排序
                """
                perPeriod = perPeriod.sort_values(['windcode'], ascending = 'True')
                
                """
                1. 每期进行截面多元WLS回归
                2. 每期选用当期滚动区间有效的单因子进行回归
                """
                results = self.__WLSRegression(perPeriod,factorList,industryList, validFactorList)
                
                if results != None:
                    #print results.summary()
                    """
                    print results.params
                    print 'the length of params'
                    print len(results.params)
                    """
               
                    """
                    规整化date类型
                    """
                    #perPeriod['Date'] =pd.to_datetime(perPeriod['Date'], format = '%Y%m%d')
                    perPeriod['Date'] =pd.to_datetime(perPeriod['Date'], format = '%Y-%m-%d')
                    changeDate = pd.DatetimeIndex(perPeriod['Date'])
                 
                    """
                    记录一期WLS回归的收益率系数,回归系数顺序：测试因子+行业因子
                    """
                    """
                    tempList = []
                    tempList.append(changeDate[0])
                    for i in range(0, len(results.params)):
                         tempList.append(results.params[i])
                 
                    df1 = pd.DataFrame([tempList], columns = columnName)
                    recordDataframe = recordDataframe.append(df1)
                    """
                    
                    """
                    记录回归得到的有效因子收益率+行业收益率
                    """
                    tempList_factor = pd.DataFrame(columns = columnName)
                    tempList_factor.loc[1,:] = None
                    tempList_factor.loc[1,'Date'] = (changeDate[0])
                    realFactorList = validFactorList + industryList
                    tempList_factor.loc[1, realFactorList] = list(results.params)
                    """
                    填充空值，对未选到的因子用0填充
                    """
                    tempList_factor = tempList_factor.fillna(0)
                    recordDataframe = recordDataframe.append(tempList_factor)
                    
              
                    """
                    记录一个特定日期每只股票的回归残差值
                    """
                    tempList_resid=pd.DataFrame(columns =columnName_resid)
                    tempList_resid.loc[1,:]=None
                    tempList_resid.loc[1,'Date']=(changeDate[0])
                    tempList_resid.loc[1,perPeriod['windcode']]=list(results.resid)
                    
                    """
                    填充空值
                    """
                    tempList_resid = tempList_resid.fillna(0)
                    recordDataframeResid = recordDataframeResid.append(tempList_resid)
                    
                                
                    """
                    print'resid test'
                    print results.resid
                    print len(results.resid)
                    print perPeriod['windcode']
                    print len(perPeriod['windcode'])
                    """
        """
        保存因子收益率
        """
        dataFileName = str(date) + '_multiFactorRecordDataframe.csv'
        recordDataframe.to_csv('dataFile/factorFile/%s'%(dataFileName), encoding = 'gbk', index = False)
        """
        保存股票残差
        """
        dataFileName = str(date) + '_recordResidual.csv'
        recordDataframeResid.to_csv("dataFile/residFile/%s"%(dataFileName), encoding='gbk',index=False)
                
        """
        返回因子收益率
        """
        return recordDataframe
                
    """
    方法一：
    以下代码用于解决异方差问题
    OLS的残差平方记为u^2.用log(u^2)对单因子和行业因子做回归，得到log(u^2)的预测值luhat^2。
    用来实现WLS的权重则为w=1/exp(luhat^2)。以w为权重做WLS （首选方法一）
    方法二：WLS使用个股流通市值的平方根作为权重，有利于消除异方差性
    """
    
    def __WLSRegression(self, perPeriod, factorList,industryList, validFactorList):
        
        """
        多元线性回归，先将industry_name列转为dummy矩阵，然后将因子和行业一起做回归
        """
        """
        未来一期收益率
        """
        logreturnDataframe = perPeriod['future_logreturn']
        """
        测试因子
        """
        factorDataframe =  perPeriod[validFactorList]
        """
        行业哑变量矩阵
        """
        industryDataframe = pd.get_dummies(perPeriod['industry_name'])
        #print industryDataframe.shape
        #print industryDataframe.columns
        """
        行业哑变量矩阵按照industryList的顺序调整列顺序
        """
        industryDataframe = pd.DataFrame(industryDataframe, columns = industryList)
        
        """
        拼接行业矩阵
        """
        factorDataframe[industryList] = industryDataframe
        """
        将Null值置为0，有时存在某期整个行业股票缺失的情况
        """
        factorDataframe = factorDataframe.fillna(0)
                
        model = sm.OLS(logreturnDataframe, factorDataframe)
        results = model.fit()
        
        """
        异方差的处理
        """
        residSquare = np.square(results.resid) 
        logResidual = np.log(residSquare)
        model2 = sm.OLS(logResidual, factorDataframe)
        results2 = model2.fit()
       
        predictValue = results2.fittedvalues
        if predictValue.isnull().any()==True:
            return None
        else:
            w = 1/(np.exp(predictValue))
            model3 = sm.WLS(logreturnDataframe, factorDataframe, weights = w)
            results3 = model3.fit()
        #return results
        return results3
            
        
    
    def __weightAveragePredictRoll(self, date,recordData,factorList,industryList, pastPeriodN):
        """
        每一期的截面用过去N期收益率的均值作为该期的预期收益率
        """
        
        totalList = factorList + industryList
        predictReturnRatio =  pd.rolling_mean(recordData[totalList], pastPeriodN)

        """
        取得最后一行，用前N期平均预测出的是第N+1期的值
        """
        predictReturnRatioNow = predictReturnRatio[-1:]
        predictReturnRatioNow['Date'] = date
        return predictReturnRatioNow
    
    
    def __EMAPredictRoll(self, date,recordData,factorList,industryList, pastPeriodN):
        
        totalList = factorList + industryList
        predictReturnRatio =  pd.ewma(recordData[totalList], span = pastPeriodN)
        
        
        predictReturnRatioNow = predictReturnRatio[-1:]
        predictReturnRatioNow['Date'] = date
        return predictReturnRatioNow

    """
    对因子收益率矩阵和股票因子值矩阵进行点乘，得到最终的股票收益率矩阵
    """
    def getStockReturn(self, factorList, pastPeriodN):
        
        """
        先清空文件夹下原有文件
        """
        if os.path.exists("dataFile/returnFile"):
            shutil.rmtree("dataFile/returnFile")
            print('remove returnFilenow')
        if not os.path.exists("dataFile/returnFile"):
            os.mkdir('dataFile/returnFile')
        
        """
        获取所有行业名称
        """
        industryList = pd.read_csv("dataFile/tempFile/industryList.csv", encoding='gbk')
        industryList = industryList["industry_name"].tolist()
        
        
        """
        读取股票因子值矩阵
        """
        valueMatrix = pd.read_csv('dataFile/tempFile/multiFactorValueDataframe.csv', encoding = 'gbk')
        """
        读取因子收益率矩阵
        """
        factorReturnMatrix = pd.read_csv('dataFile/tempFile/predictFactorDataframe.csv',encoding = 'gbk')
        
        
        valueMatrixGroup = valueMatrix.groupby('Date')
        factorReturnMatrixGroup = factorReturnMatrix.groupby('Date')
        
        """
        对每个截面的数据进行矩阵点乘运算
        """
        count = 0
        for date, group in factorReturnMatrixGroup:
            count +=1
         
            """
            if count <= pastPeriodN:
                continue
            """
            
            """
            获取每期的股票因子值
            """
            valueMatrixPerPeriod = valueMatrixGroup.get_group(date)  
            """
            获取每期的股票因子收益率
            """
            factorReturnMatrixPerPeriod = factorReturnMatrixGroup.get_group(date)
            
            """
            筛选因子列，包含因子和行业列
            """
            """
            valueMatrixPerPeriod2 = valueMatrixPerPeriod[factorList+industryList]
            factorReturnMatrixPerPeriod2 = factorReturnMatrixPerPeriod[factorList+industryList]
            """
            valueMatrixPerPeriod2 = valueMatrixPerPeriod[factorList]
            factorReturnMatrixPerPeriod2 = factorReturnMatrixPerPeriod[factorList]
            
            valueMatrixnp = np.mat(valueMatrixPerPeriod2)
            factorReturnMatrixnp = np.mat(factorReturnMatrixPerPeriod2.T)

            """
            获取每期的股票收益率
            """
            stockReturnnp = np.dot(valueMatrixnp,factorReturnMatrixnp)
            stockReturn = pd.DataFrame(stockReturnnp)
            """
            print stockReturn.shape
            print valueMatrixPerPeriod.shape
            """
            stockReturn.columns = ['return']
            valueMatrixPerPeriod = valueMatrixPerPeriod.reset_index()
            
            """
            将windcode列,industry,sec_name列拼接进去
            """
            stockReturn['windcode'] = valueMatrixPerPeriod['windcode']
            stockReturn['industry_name'] = valueMatrixPerPeriod['industry_name']
            stockReturn['sec_name'] = valueMatrixPerPeriod['sec_name']
           
            """
            调整列顺序
            """
            stockReturn = pd.DataFrame(stockReturn, columns = ['windcode','sec_name','industry_name','return'])
            
            """
            拼接industry列,用于组合规划时的行业限制 
            2017/11/24, chenxiangdong
            """
            stockReturn[industryList] = valueMatrixPerPeriod[industryList]
            stockReturn = stockReturn.set_index('windcode')
            stockReturn.to_csv('dataFile/returnFile/'+date+'_return.csv', encoding = 'gbk')

    """
    基于滚动窗口的多因子回归
    """
    def multiFactorReturnRegressionRoll(self, datSelect, factorList, periodN,forecastMethod, factorRolling):
        print("Get industry list")
        """
        获取所有行业名称列表
        """        
        industryList = []
        datIndustryGroup = datSelect.groupby("industry_name")
        for name, group in datIndustryGroup:
            if name != '':
                industryList.append(name)
                
        """
        保存行业列表到本地Excel
        """
        industryDataframe = pd.DataFrame(industryList)
        industryDataframe.columns = ['industry_name']
        industryDataframe.to_csv("dataFile/tempFile/industryList.csv", encoding='gbk',index=False)
        
    
        """
        用来记录每个截面的因子值
        Date+windcode+sec_name+factorList+industryName格式
        """
        columnValueName = ['Date', 'windcode','sec_name']
        columnValueName.extend(factorList)
        columnValueName.extend(['industry_name'])
        
        """
        columnValueName+industryList
        """
        columnValueNameIndustry = copy.deepcopy(columnValueName)
        columnValueNameIndustry.extend(industryList) 
        valueDataframe = pd.DataFrame(columns = columnValueNameIndustry)
        
        
        
        """
        用来记录每个截面的预测因子收益率, Date+factorList+industryList的数据格式
        """
        predictColumnName = []
        predictColumnName.extend(factorList)
        predictColumnName.extend(industryList)
        predictColumnName.extend(['Date'])
        predictFactorDataframe = pd.DataFrame(columns = predictColumnName)
        
        
        """
        读取每个截面的有效因子列表
        """
        validFactorFile = 'validFactors_' +str(periodN) + '.csv'
        validFactor = pd.read_csv("dataFile/validFactor/%s"%validFactorFile)
        validFactor['Date'] =pd.to_datetime(validFactor['Date'], format = '%Y/%m/%d')
        validFactor['Date'] = [pd.datetime.strftime(x, '%Y-%m-%d') for x in validFactor['Date']]
        #print validFactor['Date']
        
        count = 0
        datelistDataframe = pd.read_csv("dataFile/tempFile/dateList.csv")
        datelist = datelistDataframe['Date']
        #print datelist
        for date in datelist:
        
            count += 1
            if count <= periodN:
                print('jump this period')
                continue
                
            print(date)
            """
            此处需要注意边界问题，杜绝未来函数
            """
            """
            取得当前截面之前的所有数据
            """
            datSelectNow = datSelect[datSelect['Date'] < date]
            beginPeriod = count - (periodN+1)
            """
            print beginPeriod
            print datelist.loc[beginPeriod]
            """
            """
            获取当前截面前N期的数据
            """
            datSelectNow = datSelectNow[datSelectNow['Date'] >= datelist.loc[beginPeriod]]
            
            """
            dateListNew = list(set(datSelectNow['Date']))
            dateListNewDataframe = pd.DataFrame(dateListNew)
            dateListNewDataframe.columns = ['Date']
            dateListNewDataframe = dateListNewDataframe.sort_values('Date', ascending = True)
            print dateListNewDataframe
            """
                
       
            """
            获取当前截面的所有有效因子
            """
            
            """
            判断是否要开启滚动有效因子测试
            """
            if factorRolling == 1:
                validFactorPeriod = validFactor[validFactor['Date'] == date]   
                """
                print 'validFactor'
                print validFactorPeriod
                """
                validFactorList = []
                
                for factor in validFactorPeriod.columns:
                    #print factor
                    if validFactorPeriod[factor].any() == 1:
                        validFactorList.append(factor)
                """
                print 'validFactorList'
                print validFactorList
                """
            else:
                validFactorList = copy.deepcopy(factorList)
                #print 'use only industry'
                #validFactorList = []
                
            """
            根据选用到的有效因子，对于过去N期数据进行多元回归
            """
            recordDataframe = self.__WLSRegressionRoll(date,datSelectNow, factorList, industryList, validFactorList)
            
            
            """
            根据预测参数的不同，选择不同的预测方法
            """
            if forecastMethod  == 'MA':
                predictData = self.__weightAveragePredictRoll(date,recordDataframe, factorList, industryList, periodN)
            elif forecastMethod == 'EMA':
                predictData = self. __EMAPredictRoll(date,recordDataframe, factorList, industryList, periodN)
                 
        
            """
            获取预测后的因子值
            """
            predictFactorDataframe = predictFactorDataframe.append(predictData)
            
            
            """
            选取当期的因子数据
            """
            perPeriod = datSelect[datSelect['Date'] == date]
            valuePerPeriod = perPeriod[columnValueName]
            industryDataframe = pd.get_dummies(perPeriod['industry_name'])
            industryDataframe = pd.DataFrame(industryDataframe,columns = industryList)
            industryDataframe = industryDataframe.fillna(0)
            #valuePerPeriodIndustry = pd.merge(valuePerPeriod, industryDataframe, left_index = True, right_index = True)
            valuePerPeriod[industryList] = industryDataframe
            df2 = pd.DataFrame(valuePerPeriod, columns = columnValueNameIndustry)
            valueDataframe = valueDataframe.append(df2)
           
        
        valueDataframe.sort_values(by = ['Date','windcode'], ascending = True)
        valueDataframe = valueDataframe.set_index('Date')
        """
        保存因子值
        """
        valueDataframe.to_csv('dataFile/tempFile/multiFactorValueDataframe.csv', encoding = 'gbk')
        
        
        """
        保存因子收益率
        """
        predictFactorDataframe.sort_values(by = ['Date'], ascending = True)
        predictFactorDataframe = predictFactorDataframe.set_index('Date')
        predictFactorDataframe.to_csv('dataFile/tempFile/predictFactorDataframe.csv', encoding = 'gbk')
        
        
        
        
        
        
        
        
    #######modeified by Ruijing at 2018-01-03  ########################  
    """
    
    保存市值和行业正交后的因子值
    """    
        
    def __getOrthogonalValue(self, perPeriod, factorList):
         """
         IC值计算，需要排除市值和行业对因子暴露度影响，
         以因子暴露度为因变量，对市值因子和行业因子做回归。
         用残差替代原来的暴露值，求残差与股票预期收益率的相关系数
         """
         for factorName in factorList:
             if(factorName =='MV'):
                 modelIC = smf.ols(formula='%s ~ C(industry_name)'%(factorName), data=perPeriod)
             else:
                 modelIC = smf.ols(formula='%s ~ MV+C(industry_name)'%(factorName), data=perPeriod)
             
             resultsIC = modelIC.fit()
             perPeriod[factorName+'New'] = resultsIC.resid
         
        
         return perPeriod
        
     ####### modification end ####################   
        
        
        
        
        
        
        
        
    
    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    基于IC均值，IR均值的因子收益率预测方式
    """""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
    def __getICValue(self, perPeriod, factorName):
         """
         IC值计算，需要排除市值和行业对因子暴露度影响，
         以因子暴露度为因变量，对市值因子和行业因子做回归。
         用残差替代原来的暴露值，求残差与股票预期收益率的相关系数
         """
         
         logreturnArray = perPeriod['future_logreturn'].values
         if(factorName =='MV'):
             modelIC = smf.ols(formula='%s ~ C(industry_name)'%(factorName), data=perPeriod)
         else:
             modelIC = smf.ols(formula='%s ~ MV+C(industry_name)'%(factorName), data=perPeriod)
         
         resultsIC = modelIC.fit()
         
         """
         获得残差值
         """
         ICValue = np.corrcoef(resultsIC.resid, logreturnArray)[0][1]
       
         #print logreturnArray
         #print ICValue
         return ICValue
     
        
        
    """
    简单移动平均预测
    """
    def __weightAveragePredictIC(self, recordData,factorList, pastPeriodN):
        """
        每一期的截面用过去N期收益率的均值作为该期的预期收益率
        """
        predictReturnIC =  pd.rolling_mean(recordData[factorList], pastPeriodN)
        #predictReturnRatio[predictReturnRatio < 0] = 0
        #print predictReturnRatio.shift(1)
        #print predictReturnRatio.shift(-1)
        """
        此处需要shift(1)，避免用到当期的因子收益率去预测，规避未来函数
        """
        predictReturnIC = predictReturnIC.shift(1)
        predictReturnIC = pd.concat([predictReturnIC,recordData['Date']], axis = 1)
        predictReturnIC['Date'] =pd.to_datetime(predictReturnIC['Date'], format = '%Y-%m-%d')
        predictReturnIC.set_index('Date', inplace=True)
        """
        保存下一期的因子收益率数据
        """
        predictReturnIC.to_csv('dataFile/tempFileIC/predictReturnIC.csv', encoding = 'gbk')
        return predictReturnIC
    
    
    
    """
    ICIR加权法
    """
    def __weightAveragePredictICIR(self, recordData,factorList, pastPeriodN):
        """
        每一期的截面用过去N期收益率的均值作为该期的预期收益率
        """
        predictReturnIC =  pd.rolling_mean(recordData[factorList], pastPeriodN)
        #predictReturnRatio[predictReturnRatio < 0] = 0
        #print predictReturnRatio.shift(1)
        #print predictReturnRatio.shift(-1)
        ICstd = pd.rolling_std(recordData[factorList], pastPeriodN, min_periods = pastPeriodN)
        IR = predictReturnIC.div(ICstd)
        
        """
        此处需要shift(1)，避免用到当期的因子收益率去预测，规避未来函数
        """
        predictReturnIC = predictReturnIC.shift(1)
        predictReturnIC = pd.concat([predictReturnIC,recordData['Date']], axis = 1)
        predictReturnIC['Date'] =pd.to_datetime(predictReturnIC['Date'], format = '%Y-%m-%d')
        predictReturnIC.set_index('Date', inplace=True)
        """
        此处需要shift(1)，避免用到当期的因子收益率去预测，规避未来函数
        """
        ICstd = ICstd.shift(1)
        ICstd = pd.concat([ICstd,recordData['Date']], axis = 1)
        ICstd['Date'] =pd.to_datetime(ICstd['Date'], format = '%Y-%m-%d')
        ICstd.set_index('Date', inplace=True)
        """
        此处需要shift(1)，避免用到当期的因子收益率去预测，规避未来函数
        """
        IR = IR.shift(1)
        IR = pd.concat([IR,recordData['Date']], axis = 1)
        IR['Date'] =pd.to_datetime(IR['Date'], format = '%Y-%m-%d')
        IR.set_index('Date', inplace=True)
        """
        保存下一期的因子收益率数据
        """
        predictReturnIC.to_csv('dataFile/tempFileIC/predictReturnIC.csv', encoding = 'gbk')
        """
        保存下因子的滚动波动率
        """
        ICstd.to_csv('dataFile/tempFileIC/ICStd.csv', encoding = 'gbk')
        """
        保存预测IR值
        """
        IR.to_csv('dataFile/tempFileIC/predictReturnIR.csv', encoding = 'gbk')
        
        return predictReturnIC
        
    
    """
    指数移动平均预测
    """
    def __EMAPredictIC(self, recordData,factorList, pastPeriodN):
        """
        每一期的截面用过去N期收益率的均值作为该期的预期收益率
        """
        predictReturnIC = pd.ewma(recordData[factorList],span = pastPeriodN, min_periods = pastPeriodN)
        """
        收益率为负数的地方强制设置为0
        """
        #predictReturnRatio[predictReturnRatio < 0] = 0          
        """
        此处需要shift(1)，避免用到当期的因子收益率去预测，规避未来函数
        """
        predictReturnIC = predictReturnIC.shift(1)
        predictReturnIC = pd.concat([predictReturnIC,recordData['Date']], axis = 1)
        predictReturnIC['Date'] =pd.to_datetime(predictReturnIC['Date'], format = '%Y-%m-%d')
        predictReturnIC.set_index('Date', inplace=True)
        """
        保存下一期的因子收益率数据
        """
        predictReturnIC.to_csv('dataFile/tempFileIC/predictReturnIC.csv', encoding = 'gbk')
        return predictReturnIC
    
    
    
    def multiFactorReturnRegressionRollIC(self, datSelect, factorList, periodN,forecastMethod, factorRolling, Org):
    
        print("Get industry list")
        """
        获取所有行业名称列表
        """        
        industryList = []
        datIndustryGroup = datSelect.groupby("industry_name")
        for name, group in datIndustryGroup:
            if name != '':
                industryList.append(name)
        """
        保存行业列表到本地Excel
        """
        industryDataframe = pd.DataFrame(industryList)
        industryDataframe.columns = ['industry_name']
        industryDataframe.to_csv("dataFile/tempFile/industryList.csv", encoding='gbk',index=False)
        
        """
        用来记录每个截面的因子值
        Date+windcode+sec_name+factorList+industryName格式
        """
        columnValueName = ['Date', 'windcode','sec_name', 'MV','bench_weight']
        #columnValueName = ['Date', 'windcode']
        ##################### modified by Ruijing at 2018-01-03 ##########
        factorListNew = []
        for factorName in factorList:
            factorListNew.append(factorName+'New')
        if Org == 1:
            columnValueName.extend(factorListNew)
        ##################### modification end ###########################
        else:
            columnValueName.extend(factorList)
            
        columnValueName.extend(['industry_name'])
        
        """
        columnValueName+industryList
        """
        columnValueNameIndustry = copy.deepcopy(columnValueName)
        columnValueNameIndustry.extend(industryList) 
        valueDataframe = pd.DataFrame(columns = columnValueNameIndustry)
        
        """
        用来记录每个截面的因子收益率, Date+factorList+industryList的数据格式
        """
        ICColumnName = ['Date']
        ICColumnName.extend(factorList)
 
        ICDataframe = pd.DataFrame(columns =ICColumnName)
        
        
        """
        按照每个截面分组回归
        """
        datPerPeriod = datSelect.groupby("Date")
        print(datSelect['Date'])
        datPerPeriod_groups = datSelect.groupby("Date").groups                
        for name, group in datPerPeriod:
            print(name)
            if name in datPerPeriod_groups:
                perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                """
                规整化date类型
                """
                perPeriod['Date'] =pd.to_datetime(perPeriod['Date'], format = '%Y-%m-%d')
           
                changeDate = pd.DatetimeIndex(perPeriod['Date'])
                tempList = []
                tempList.append(changeDate[0])
                """
                遍历所有单因子：
                """
                for factorName in factorList:                                  
                    ICValue = self.__getICValue(perPeriod, factorName)
                    tempList.append(ICValue)
                    
                df1 = pd.DataFrame([tempList], columns = ICColumnName)
                ICDataframe = ICDataframe.append(df1)
                
                if Org == 1:
                    perPeriod = self.__getOrthogonalValue(perPeriod, factorList)
                    
                """
                记录每个截面的因子值
                """
                valuePerPeriod = perPeriod[columnValueName]
                industryDataframe = pd.get_dummies(perPeriod['industry_name'])
                industryDataframe = pd.DataFrame(industryDataframe,columns = industryList)
                industryDataframe = industryDataframe.fillna(0)
                valuePerPeriod[industryList] = industryDataframe
                df2 = pd.DataFrame(valuePerPeriod, columns = columnValueNameIndustry)
                valueDataframe = valueDataframe.append(df2)
           
        """
        保存因子值
        """
        valueDataframe.sort_values(by = ['Date','windcode'], ascending = True)
        valueDataframe = valueDataframe.set_index('Date')
        if Org == 1:
            ##################### modified by Ruijing at 2018-01-03 ##########
            valueDataframe.to_csv('dataFile/tempFile/multiFactorValueDataframeNew.csv', encoding = 'gbk')
            ##################### modification end ###########################
        else:
            
            valueDataframe.to_csv('dataFile/tempFile/multiFactorValueDataframe.csv', encoding = 'gbk')
        
        """
        保存IC值
        """
        ICDataframe.to_csv('dataFile/tempFileIC/ICList.csv', index = False)
        #ICDataframe = pd.read_csv('dataFile/tempFileIC/ICList.csv', encoding = 'gbk')
        """
        根据参数判断选用的预测方法:指数移动平均或者简单平均
        """
        if forecastMethod == 'EMA':
            self.__EMAPredictIC(ICDataframe,factorList, periodN)
        elif forecastMethod == 'MA':
            self.__weightAveragePredictIC(ICDataframe,factorList, periodN)
            
            
        """
        elif forecastMethod == 'MA_IR':
            self.__weightAveragePredictICIR(ICDataframe,factorList, periodN)
        """
        
    
       
    """
    获得由IC预测的股票收益率
    """
    def getStockReturnIC(self, factorList, pastPeriodN, factorRolling, indexBench, weightingMethod,Org):
        """
        先清空文件夹下原有文件
        """
        if os.path.exists("dataFile/returnFile"):
            shutil.rmtree("dataFile/returnFile")
            print('remove returnFilenow')
        if not os.path.exists("dataFile/returnFile"):
            os.mkdir('dataFile/returnFile')
        """
        获取所有行业名称
        """
        """
        industryList = pd.read_csv("dataFile/tempFile/industryList.csv", encoding='gbk')
        industryList = industryList["industry_name"].tolist()
        """
        """
        读取股票因子值矩阵
        """
        if Org == 1:
            ##################### modified by Ruijing at 2018-01-03 ##########
            valueMatrix = pd.read_csv('dataFile/tempFile/multiFactorValueDataframeNew.csv', encoding = 'gbk')
            ##################### modification end ###########################
        else:
            valueMatrix = pd.read_csv('dataFile/tempFile/multiFactorValueDataframe.csv', encoding = 'gbk')
        
        """
        读取因子收益率矩阵
        """
        factorReturnMatrix = pd.read_csv('dataFile/tempFileIC/predictReturnIC.csv',encoding = 'gbk')


        valueMatrixGroup = valueMatrix.groupby('Date')
        factorReturnMatrixGroup = factorReturnMatrix.groupby('Date')
        """
        读取每个截面的有效因子列表
        """
        """
        validFactorFile = 'validFactors_'  +str(indexBench) +'_'+str(pastPeriodN) + '.csv'
        validFactor = pd.read_csv("dataFile/validFactor/%s"%validFactorFile)
        validFactor['Date'] =pd.to_datetime(validFactor['Date'], format = '%Y/%m/%d')
        validFactor['Date'] = [pd.datetime.strftime(x, '%Y-%m-%d') for x in validFactor['Date']]
        """
        
        ########### modified by Ruijing at 2018-01-03 ############
        factorListNew = []
        for factorName in factorList:
            factorListNew.append(factorName+'New')
        ############### modification end ########################
        
        """
        对每个截面的数据进行矩阵点乘运算
        """
        count = 0
        for date, group in factorReturnMatrixGroup:
            print(date)
            count +=1
            if count <= pastPeriodN:
                continue
            
            """
            获取每期的股票因子值
            """
            valueMatrixPerPeriod = valueMatrixGroup.get_group(date)  
            """
            获取每期的股票因子收益率
            """
            factorReturnMatrixPerPeriod = factorReturnMatrixGroup.get_group(date)
            """
            筛选因子列
            """
            if Org == 1:
                valueMatrixPerPeriod2 = valueMatrixPerPeriod[factorListNew]
            else:
                    
                valueMatrixPerPeriod2 = valueMatrixPerPeriod[factorList]
                
            factorReturnMatrixPerPeriod2 = factorReturnMatrixPerPeriod[factorList]
            
            
            valueMatrixnp = np.mat(valueMatrixPerPeriod2)
            
            """
            判断是否是等权加权还是IC加权
            """
            if weightingMethod == 'equal':
                """
                print factorReturnMatrixPerPeriod2
                factorReturnMatrixPerPeriod2[factorReturnMatrixPerPeriod2 < 0] = -1
                factorReturnMatrixPerPeriod2[factorReturnMatrixPerPeriod2 >= 0] = 1
                """
                
                """
                所有四因子都是正向的
                """
                print(factorReturnMatrixPerPeriod2)
                factorReturnMatrixPerPeriod2[factorReturnMatrixPerPeriod2 < 0] = 1
                factorReturnMatrixPerPeriod2[factorReturnMatrixPerPeriod2 >= 0] = 1
                print(factorReturnMatrixPerPeriod2)
                
            elif weightingMethod == 'IC':
                pass
            
            
            """
            选取区间有效因子
            """
            if factorRolling == 1:
                
                validFactorFile = 'validFactors_'  +str(indexBench) +'_'+str(pastPeriodN) + '.csv'
                validFactor = pd.read_csv("dataFile/validFactor/%s"%validFactorFile)
                validFactor['Date'] =pd.to_datetime(validFactor['Date'], format = '%Y/%m/%d')
                validFactor['Date'] = [pd.datetime.strftime(x, '%Y-%m-%d') for x in validFactor['Date']]
                
                
                validFactorPeriod = validFactor[validFactor['Date'] == date]   
                validFactorList = validFactorPeriod[factorList]
              
                factorReturnMatrixPerPeriod2np = np.mat(factorReturnMatrixPerPeriod2)
                validFactorListnp = np.mat(validFactorList)
                print('factorReturn')
                print(factorReturnMatrixPerPeriod2np)
                print('validFactor')
                print(validFactorListnp)
              
                """
                矩阵对应位置相乘， 未入选的IC对应validFactor位置为0
                """
                factorReturnMatrixnp = np.multiply(factorReturnMatrixPerPeriod2np,validFactorListnp)
                factorReturnMatrixnp = np.mat(factorReturnMatrixnp.T)
                print('after multipy')
                print(factorReturnMatrixnp)
                
            else:
                factorReturnMatrixnp = np.mat(factorReturnMatrixPerPeriod2.T)
                
                
            """
            获取每期的股票收益率
            """
            stockReturnnp = np.dot(valueMatrixnp,factorReturnMatrixnp)
            stockReturn = pd.DataFrame(stockReturnnp)
            """
            print stockReturn.shape
            print valueMatrixPerPeriod.shape
            """
            stockReturn.columns = ['return']
            valueMatrixPerPeriod = valueMatrixPerPeriod.reset_index()
            """
            将windcode列,industry,sec_name列拼接进去
            """
            stockReturn['windcode'] = valueMatrixPerPeriod['windcode']
            stockReturn['industry_name'] = valueMatrixPerPeriod['industry_name']
            stockReturn['sec_name'] = valueMatrixPerPeriod['sec_name']
            stockReturn['bench_weight'] = valueMatrixPerPeriod['bench_weight']
            stockReturn['MV'] = valueMatrixPerPeriod['MV']
           
            """
            调整列顺序
            """
            stockReturn = pd.DataFrame(stockReturn, columns = ['windcode','sec_name','industry_name','return', 'MV','bench_weight'])
            #stockReturn = pd.DataFrame(stockReturn, columns = ['windcode','industry_name','return'])
            stockReturn = stockReturn.set_index('windcode')
            stockReturn.to_csv('dataFile/returnFile/'+date+'_return.csv', encoding = 'gbk')
            
    
    """
    每期的有效IC * 因子值得到一个股票复合收益率因子
    1.股票按照行业分组，按照股票收益率从大到小排列
    2.100*行业权重，确定每个行业中应选股票数量。从大到小选取
    3. 每只股票权重设置： 个股符合收益率/（sum(所有股票收益率)）
    
    其他：
    此处需要注意复合收益率为负数的情况
    """
    def getStockWeightIndustryNeutral(self,datSelect, periodN, forecastMethod, indexBench, weightingMethod,Org,factorList,groupChoose,selectRatio,freq,reduceIndustry,IndustryNum):
        
        
        
        """
        读取行业权重文件，按月resample，取月初数据
        """
        if indexBench == 'ZZ500':
            industryWeight = pd.read_csv("dataFile/PART2/index/industry_weight_ZZ500.csv", encoding = 'gbk')
        elif indexBench == 'HS300' or indexBench == '':
            industryWeight = pd.read_csv("dataFile/PART2/index/industry_weight_HS300.csv", encoding = 'gbk')
        elif indexBench == 'SZ180':
            industryWeight = pd.read_csv("dataFile/PART2/index/industry_weight_SZ180.csv", encoding = 'gbk')
        elif indexBench == 'ZZ100':
            industryWeight = pd.read_csv("dataFile/PART2/index/industry_weight_ZZ100.csv", encoding = 'gbk')
        """
        对于行业权重按照月份截面，空值填充为0
        """
        industryWeight['Date'] = pd.to_datetime(industryWeight['Date'], format = '%Y-%m-%d')
        industryWeight.set_index('Date', inplace = True)
        industryWeight = industryWeight.resample('MS', fill_method = 'ffill')
        industryWeight = industryWeight.fillna(0)
        #print industryWeight
        industryWeight.reset_index(inplace = True)
        industryWeight['Date'] = [pd.datetime.strftime(x, '%Y-%m') for x in industryWeight['Date']]
        #print industryWeight
        
        if indexBench == 'ZZ500':
            industryWeight.to_csv("dataFile/PART2/index/industry_weight_ZZ500_resample.csv", encoding = 'gbk')
        elif indexBench == 'HS300' or indexBench == '':
            industryWeight.to_csv("dataFile/PART2/index/industry_weight_HS300_resample.csv", encoding = 'gbk')
        industryWeight.set_index('Date', inplace = True)
        
   
        """
        #industryWeight = industryWeight * 100
        #stockNumIndustry = industryWeight.floordiv(100)
    
        #stockNumIndustry += 1
        #print stockNumIndustry
        
        if indexBench == 'ZZ500':
            stockNumIndustry.to_csv("dataFile/index/stockNumIndex500.csv", encoding = 'gbk')
        elif indexBench == 'HS300':
            stockNumIndustry.to_csv("dataFile/index/stockNumIndex300.csv", encoding = 'gbk')
        """
        
        """
        读取每期的return文件
        """
        returnFilePath = 'dataFile/returnFile/'
        datelist = pd.read_csv('dataFile/tempFile/dateList.csv',encoding='gbk')
        datelist =  datelist[periodN:]

        """
        求得非线性规划得到的每期权重
        """
        weightFinal = pd.DataFrame()
        ###############################modification start#######################
        ##Ruijing Yang 2018-01-10##筛选行业
        """
        筛选行业操作
        """
        if reduceIndustry == 1:
            OPPToMV_industry = pd.read_csv("dataFile/tempFile/OPPToMV_industry"+indexBench+".csv",encoding = 'gbk',header = 1)
            OPPToMV_industry = OPPToMV_industry.drop(0)
            OPPToMV_industry.rename(columns = {"industry_name":"Date"},inplace = True)
            OPPToMV_industry.index = OPPToMV_industry['Date']
            
        #############################modification end###########################
        """
        逐期读取returnFile
        """
        for date in datelist['Date']:
            
         
            dateTime = pd.to_datetime(date, format = '%Y-%m-%d')
          
            dateMonth = pd.datetime.strftime(dateTime, '%Y-%m')
            
            returnFile = date+'_return.csv'
            returnFile = returnFilePath + returnFile
            datReturn = pd.read_csv(returnFile, encoding = 'gbk')
            
            if reduceIndustry == 1:
                industryList = OPPToMV_industry.ix[date]
                industryList = pd.DataFrame(industryList)
                industryList.reset_index(inplace = True)
            
                industryList = industryList.drop(0)
                industryList.rename(columns = {"index":"industry_name",date:"OPPToMV"},inplace= True)
                
                industryList.sort_values('OPPToMV', inplace = True,ascending = True)
      
                industryList.dropna(inplace = True)
                industryList.reset_index(inplace = True)
                #print industryList
                industryList = industryList.loc[IndustryNum:,'industry_name']
                #print industryList
                datReturn = datReturn[datReturn['industry_name'].isin(industryList)]
               
            
            '''
            datReturn = datSelect[datSelect['Date'] == date]
            '''
            """
            按照行业分类
            """
            datPerPeriod = datReturn.groupby("industry_name")
            datPerPeriod_groups = datReturn.groupby("industry_name").groups
            """
            需要保存因子收益率矩阵 以及 因子值矩阵
            """  
            stockSelectMonth = pd.DataFrame()                     
            for name, group in datPerPeriod:
                if name in datPerPeriod_groups:
                    #print name
                    perPeriod = datPerPeriod.get_group(name)  
                    
                    '''
                    if name == u'银行(中信)' or name == u'非银行金融(中信)':
                        print name
                        perPeriod = perPeriod.sort_values('MV', ascending = False)
                    else:
                        """
                        按照return从大到小排列
                        """
                        perPeriod = perPeriod.sort_values('return', ascending = False)
                    '''
                    perPeriod = perPeriod.sort_values('return', ascending = False)
                    
                    """
                    #print perPeriod
                    print 'stocks num in industry is'
                    print perPeriod.shape[0]
                    """
                    
                    #stockNumSelect = int (0.3 * perPeriod.shape[0]) + 1
                    stockNumSelect = math.ceil(selectRatio * perPeriod.shape[0])
                    
                    """
                    print 'stock num in industry to select is'
                    print stockNumSelect
                    """
                    if groupChoose == 2:
                        perPeriod = perPeriod[int(stockNumSelect):(2*int(stockNumSelect))]
                    elif groupChoose == 1:
                        perPeriod = perPeriod[:int(stockNumSelect)]
                    elif groupChoose == 3:
                        perPeriod = perPeriod[int(2*stockNumSelect):(3*int(stockNumSelect))]
                    elif groupChoose == 4:
                        perPeriod = perPeriod[int(3*stockNumSelect):(4*int(stockNumSelect))]
                    elif groupChoose == 5:
                        perPeriod = perPeriod[int(4*stockNumSelect):]
                        
                    
                    
                    
                    weightIndus = (industryWeight.loc[dateMonth, name])/100.0
                    
                    
                       
#        ################################### modification start ###################################  
#        '''
#        Modified by Xin-Ji Liu on 20180102
#        buy stocks according to their original weight (multiply one constant), rather than equal weight
#        '''
                    weight_cur_sum = sum(perPeriod['bench_weight'])
                    """
                    print 'industry weight is'
                    print weightIndus
                    """
                    
                    if stockNumSelect > 0:
                        perPeriod['weight'] = perPeriod['bench_weight']*(weightIndus/weight_cur_sum)
                        stockSelectMonth = stockSelectMonth.append(perPeriod)
#        ################################### modification end ################################### 
                    
                    """
                    print 'industry weight is'
                    print weightIndus
                    """
                    '''
                    if stockNumSelect > 0:
                        perPeriod['weight'] = weightIndus/stockNumSelect
                        stockSelectMonth = stockSelectMonth.append(perPeriod)
                    
                    '''
     
            stockSelectMonth.sort_values('return', ascending = False)
            stockSelectMonth['Date'] = date
            ################################### modification start ################################### 
            ##如果仓位不满100%，按比例扩展
            weightSum_ALL = sum(stockSelectMonth['weight'])
            stockSelectMonth['weight']= stockSelectMonth['weight']/weightSum_ALL
            ################################### modification end ################################### 
            weightFinal = weightFinal.append(stockSelectMonth)
        
        
        """
        做一次dropna处理，删除一些无因子入选的情况
        """
        weightFinal = weightFinal.dropna()
        fileName = ''
        for factorName in factorList:
            fileName = fileName + factorName
            
        if Org == 1:
            weightOutputName = 'weightFinalNotEqualOrg_' + str(indexBench) + '_' +str(weightingMethod) + '_' +str(forecastMethod) + '_' + str(periodN) + '_' + fileName + '_group' + str(groupChoose)+'.csv'
        else:
            weightOutputName = 'weightFinalNotEqual_' + str(indexBench) + '_' +str(weightingMethod) + '_' +str(forecastMethod) + '_' + str(periodN) + '_' +str(1/selectRatio) + '_' +fileName + '.csv'

        weightFinal.to_csv('dataFile/PART2/weightFile/weightFileFinalOutput/'+weightOutputName, encoding = 'gbk', index = False)
            
    
    
    
    
   

    


def main():

    

    """
    本地测试或者服务器测试
    """
    localOrServer = 1
    """
    使用因子收益率预测收益率/IC预测收益率/等权方式加权
    0:因子收益率加权
    1：IC加权
    2：等权加权
    """
    #weightingMethod = 'IC'
    #weightingMethod = 'factor'
    #weightingMethod = 'equal'
    weightingMethodList = ['equal']
    """
    是否开启风险模型
    """
    useRiskModel = 0
    """
    是否开启滚动测试
    """
    factorRolling = 0
    
    MultiFactor = MultiFactorReturnRiskModel(localOrServer)
    """
    多因子模型输入参数：
    时间区间
    选用的因子
    选用的预测方法
    用来预测的期数
    """
    #factorList = ['PB','logreturn1Month','returnStdOneMonth','meanTurnOneMonth','MV']
    """
    factorList = [ 'MV', 'meanTurnOneMonth', 'returnStdOneMonth',
                  'logreturn1Month','logreturn3Month', 'logreturn6Month', 'logreturn12Month',
                  'netProfit_TTM', 'revGrowth_season', 'ROE_TTM', 'PB', 'PE_TTM',
                  'EBITDAEV', 'EBITEV', 'debtToAssets_TTM', 'EquityToDebt','equityMultiplier_TTM','OPPToMV']
    """
    #factorList = ['returnStd6Month', 'OPPToMV','DPS','revGrowth_season','ROEGrowth_season','br','AssetTurnover_season','periodCostRate_TTM','capitalReturn']
    #factorList = ['PE_TTM', 'OPPToMV','DPS','trix','meanTurn6Month','netProfit_TTM','ROE_TTM','fcffToMV_TTM','EBITEV','EBITDAEV']
    #factorList = [['OPPToMV','PB'],['ROE_TTM','PB'],['OPPToMV','ROE_TTM'],['OPPToMV','MV'],['OPPToMV','ROE_TTM','PB'],['OPPToMV','PE_TTM'],['OPPToMV','PE_TTM','PB']]
    
    #factorList = ['PE_TTM', 'OPPToMV','DPS','trix','meanTurn6Month','netProfit_TTM','ROE_TTM','PB','EBITDAEV','EBITEV']
    forecastMethodList = ['MA']
    periodList = [1]
    
    """
    此处注意csv文件和hdf文件的日期格式不同
    """
    """
    beginDate = '2009-01-01'
    endDate = '2017-06-01'
    """
    
    beginDate = '2004-01-01'
    endDate = '2017-06-01'
    indexBench = 'HS300'
    #indexBench = ''
    """
    如果用正交因子，Org = 1
    """
    Org = 1
    
    """
    需要进行回测的因子
    """
    factorList = ['OPPToMV']
    """
    如果要选排名第二组的股票，则groupChoose = 2，否则为1
    """
    groupChooseList = [1,2,3,4,5]
    #groupChoose = 1
    
    
  
    """
    是否筛选行业的参数，如果reduceIndustry = 0，就不筛选行业，如果reduceIndustry = 1，则筛选行业，下面的IndustryNumList才需要赋值，否则IndustryNum = 0
    """
    reduceIndustry = 0
    IndustryNumList = [3,6,9]
    IndustryNum = 0
    """
    如果需要周频调仓，改成week
    """
    freq = 'month'
    """
    选股比例的设置，默认是1/3
    """
    #selectRatioList = [1/4]
    selectRatio = 1/5
    """
    选取有效的数据:
    1. 选取指定区间
    2. 选取指定列
    3. 选取月初截面数据，保存日期列表
    4. 数据归一化预处理 
    """
    '''
    """
    PART1,数据预处理
    """
    
    """
    全区间因子列表
    """
    factorList = ['MV','meanTurnOneMonth', 'meanTurn3Month', 'meanTurn6Month','meanTurn12Month', 'returnStdOneMonth', 'returnStd3Month','returnStd6Month', 'returnStd12Month', 'logreturn1Month','logreturn3Month', 'logreturn6Month', 'logreturn12Month', 'OCF','DPS','netProfit_TTM','revGrowth_TTM','netProfitGrowth_TTM','PB','PE_TTM','PEG_TTM','fcffToMV_TTM','grossProfitMargin_TTM','netProfitMargin_TTM','totalAssetTurnover_TTM','periodCostRate_TTM','currentRatio_TTM','ROE_TTM','cashToRevenue_TTM','mainBusinessRate_TTM','debtToAssets_TTM','equityMultiplier_TTM','PS_TTM','cashToProfit_TTM','ROA_TTM','cashGrowth_TTM','ROE_season','ROEGrowth_TTM','assetToEquity_TTM','ROA_season','grossProfitMargin_season','netProfitMargin_season','AssetTurnover_season','cashToProfit_season','revGrowth_season','netProfitGrowth_season','cashGrowth_season','ROEGrowth_season','incomeTaxRate','quickRatio','cashToEarnings','capitalReturn','interestCover','LTdebtToWorkCapital','depositReceivedToRev','equityRatio','shareHoldersEqRatio','SHEqToAsset','proportionOfFixIncome','LIQDR','proportionOfMobIncome','EquityToDebt','CashRatio','debtToTanAssets','invenTurnoverRatio','invenToSales','recevTurnoverRatio','mobAssetTurnoverRatio','fixAssetTurnoverRatio','EBITDAEV','EBITEV','OPPToMV']

    """
    500的因子列表
    """
    #factorList = ['returnStd6Month', 'OPPToMV','DPS','revGrowth_season','ROEGrowth_season','ROE_TTM','PB','capitalReturn','AssetTurnover_season','MV']
    """
    300的因子列表
    """
    #factorList = ['PE_TTM', 'OPPToMV','DPS','trix','meanTurn6Month','netProfit_TTM','ROE_TTM','PB','EBITEV','EBITDAEV','MV']
    """
    180和100的因子列表
    """
    #factorList = ['PE_TTM', 'OPPToMV','DPS','netProfitMargin_season','ROA_season','PB','MV']

    if freq == 'month':
        datSelect = MultiFactor.monthlyDataSelect(beginDate, endDate, factorList, indexBench)
    
        print datSelect.shape
    
        """
        将预处理后的月截面数据保存为中间文件，避免重复处理
        """
        if indexBench == 'ZZ500':
            datSelect.to_csv('dataFile/PART2/index/datIndex_zz500_monthly.csv', encoding = 'gbk')
        elif indexBench == 'HS300':
            datSelect.to_csv('dataFile/PART2/index/datIndex_hs300_monthly.csv', encoding = 'gbk')
        elif indexBench == 'SZ180':
            datSelect.to_csv('dataFile/PART2/index/datIndex_sz180_monthly.csv', encoding = 'gbk')
        elif indexBench == 'ZZ100':
            datSelect.to_csv('dataFile/PART2/index/datIndex_zz100_monthly.csv', encoding = 'gbk')
        elif indexBench == '':
            datSelect.to_csv('dataFile/PART2/index/data_monthly.csv', encoding = 'gbk')
            
            
    elif freq == 'week':
        datSelect = MultiFactor.weeklyDataSelect(beginDate, endDate, factorList, indexBench)
    
        print datSelect.shape
    
        """
        将预处理后的月截面数据保存为中间文件，避免重复处理
        """
        if indexBench == 'ZZ500':
            datSelect.to_csv('dataFile/PART2/index/datIndex_zz500_monthly.csv', encoding = 'gbk')
        elif indexBench == 'HS300':
            datSelect.to_csv('dataFile/PART2/index/datIndex_hs300_monthly.csv', encoding = 'gbk')
        elif indexBench == 'SZ180':
            datSelect.to_csv('dataFile/PART2/index/datIndex_sz180_monthly.csv', encoding = 'gbk')
        elif indexBench == 'ZZ100':
            datSelect.to_csv('dataFile/PART2/index/datIndex_zz100_monthly.csv', encoding = 'gbk')
        elif indexBench == '':
            datSelect.to_csv('dataFile/PART2/index/data_monthly.csv', encoding = 'gbk')
    
    '''
    """
    PART2,单因子回测 
    """
    """
    读取预处理好的月截面数据
    """
    if freq == 'month':
        
        if indexBench == 'ZZ500':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_zz500_monthly.csv', encoding = 'gbk')
        elif indexBench == 'HS300':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_hs300_monthly.csv', encoding = 'gbk')
        elif indexBench == 'SZ180':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_sz180_monthly.csv', encoding = 'gbk')
        elif indexBench == 'ZZ100':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_zz100_monthly.csv', encoding = 'gbk')
        elif indexBench == '':
            datSelect = pd.read_csv('dataFile/PART2/index/data_monthly.csv', encoding = 'gbk')
            
            datSelect = datSelect[datSelect['Date']>beginDate]
            datSelect['bench_weight'] = 1
            
            #datSelect.sort_values(by = ['Date','windcode'],ascending = True,inplace = True)
            #print datSelect.loc[datSelect['Date'] == '2016-08-01','future_logreturn'].values
    
    elif freq == 'week':
        if indexBench == 'ZZ500':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_zz500_weekly.csv', encoding = 'gbk')
        elif indexBench == 'HS300':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_hs300_weekly.csv', encoding = 'gbk')
        elif indexBench == 'SZ180':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_sz180_weekly.csv', encoding = 'gbk')
        elif indexBench == 'ZZ100':
            datSelect = pd.read_csv('dataFile/PART2/index/datIndex_zz100_weekly.csv', encoding = 'gbk')
        elif indexBench == '':
            datSelect = pd.read_csv('dataFile/PART2/index/data_weekly.csv', encoding = 'gbk')
    """
    对参数进行遍历测试,先遍历因子预测方法，再遍历用来预测因子收益率的期数
    """
    for forecastMethod in forecastMethodList:
        for pastPeriodN in periodList:
            for weightingMethod in weightingMethodList:
                print(forecastMethod, pastPeriodN, weightingMethod)
                if weightingMethod == 'factor': #因子收益率加权
                    print('begin multifactor regression, return model, use factor return')
                    MultiFactor.multiFactorReturnRegressionRoll(datSelect, factorList, pastPeriodN,forecastMethod, factorRolling)
                    MultiFactor.getStockReturn(factorList,pastPeriodN)
                elif weightingMethod == 'IC': #IC加权
                    print('begin multifactor regression, return model, use factor IC')
                    MultiFactor.multiFactorReturnRegressionRollIC(datSelect, factorList, pastPeriodN,forecastMethod, factorRolling,Org)
                    MultiFactor.getStockReturnIC(factorList,pastPeriodN, factorRolling, indexBench, weightingMethod,Org)
                    MultiFactor.getStockWeightIndustryNeutral(pastPeriodN, forecastMethod, indexBench, weightingMethod,Org)
                elif weightingMethod == 'equal': #等权加权
                    print('begin multifactor regression, return model, use equal weight')
                    for factorList1 in factorList:
                        factorList1 = [factorList1]
                        for groupChoose in groupChooseList:
                            MultiFactor.multiFactorReturnRegressionRollIC(datSelect, factorList1, pastPeriodN,forecastMethod, factorRolling,Org)
                            MultiFactor.getStockReturnIC(factorList1,pastPeriodN, factorRolling, indexBench, weightingMethod,Org)
                            MultiFactor.getStockWeightIndustryNeutral(datSelect,pastPeriodN, forecastMethod, indexBench, weightingMethod,Org,factorList1,groupChoose,selectRatio,freq,reduceIndustry, IndustryNum)
                    
    
   
    
    
    
     
      
"""
等分仓位进行单因子回测的逻辑
"""

def mergeWeightFile(factorName1,factorName2):
        weightFileCombine = pd.DataFrame()
        
        weightFile1 = pd.read_csv('dataFile/weightFile/weightFileFinalOutput/weightFinalNotEqualOrg_HS300_equal_MA_1'+factorName1+'.csv',encoding = 'gbk')
        
        weightFile2 = pd.read_csv('dataFile/weightFile/weightFileFinalOutput/weightFinalNotEqualOrg_HS300_equal_MA_1'+factorName2+'.csv',encoding = 'gbk')
        
        weightFile1['weight'] = weightFile1['weight']/2
        weightFile2['weight'] = weightFile2['weight']/2
        weightFile1.rename(columns = {"weight":"weight1"},inplace = True)
        weightFile2.rename(columns = {"weight":"weight2"},inplace = True)
       
        weightFileCombine = pd.merge(weightFile1,weightFile2,how = 'outer',on = ['windcode','Date','sec_name','industry_name','MV'])
        
        weightFileCombine = weightFileCombine.fillna(0)
        
        weightFileCombine['weight'] = weightFileCombine['weight1'] + weightFileCombine['weight2']
        del weightFileCombine['weight1']
        del weightFileCombine['weight2']
        weightFileCombine = weightFileCombine.sort_values(by = ['Date'])
        weightFileCombine.to_csv('dataFile/weightFile/weightFileFinalOutput/weightFinalNotEqualOrg_HS300_equal_MA_1'+factorName1+factorName2+'sep.csv',encoding = 'gbk', index = False)
       
        
        

if __name__ == "__main__":
    main()
    
    
    
    
    '''
    """
    筛选行业部分，得到行业的OPP值，用整体法计算
    """
    localOrServer = 0
    MultiFactor = MultiFactorReturnRiskModel(localOrServer)
    MultiFactor.selectIndustry_process()

    """
    等分仓位进行单因子回测的逻辑
    """
    factorName1 = 'OPPToMV'
    factorName2 = 'ROE_TTM'
    mergeWeightFile(factorName1,factorName2)       
    '''
    """
    自定义datelist
    """
    '''
    datSelect = pd.read_csv('dataFile/PART2/index/data_monthly.csv', encoding = 'gbk')
    datelistDataframe = list(set(datSelect['Date']))
    datelistDataframe = pd.DataFrame(datelistDataframe,columns = ['Date'])
    datelistDataframe = datelistDataframe.sort_values('Date',ascending = True)
    datelistDataframe = datelistDataframe[datelistDataframe['Date']>='2005-05-01']
    datelistDataframe.to_csv("dataFile/tempFile/dateList.csv", encoding='gbk',index=False)
    '''
    
    
    

