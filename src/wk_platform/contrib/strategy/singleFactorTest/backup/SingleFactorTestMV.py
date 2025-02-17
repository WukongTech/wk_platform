# -*- coding: utf-8 -*-
"""
Created on Fri Sep 08 22:33:13 2017

@author: cxd
"""



  #执行浮点除法
import pandas as pd
import numpy as np
import time
import statsmodels.api as sm
import statsmodels.formula.api as smf
from scipy import stats



"""
回归法检验单因子有效性代码
"""
class SingleFactorAnalyze():
    
    def __init__(self):
        self.__datVal = pd.DataFrame()
        
    """
    读取价格和财务指标数据，预处理完的数据
    实际应该增加预处理代码
    """
    def readData(self):
        print(("data begin load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
       
      
        """
        本机路径
        """
        #self.__datVal = pd.read_hdf('C://platformData/priceAndFactorData/dataForRegressionPost.h5')
        """
        服务器路径
        """
        self.__datVal = pd.read_hdf('/home/chenxd/platformData/priceAndFactorData/dataForRegressionPost.h5')
        
        
        print(("data after load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
        
        """
        时间区间选取
        """
        self.__datVal = self.__datVal[self.__datVal['Date'] >= '2015-01-01']
        
        print(self.__datVal.columns)
      
        
    
    def __selectDataColumns(self):
        
  
   
        datSelect = self.__datVal[['Date', 'windcode', 'industry_name','Volume','MV','future_return','future_logreturn','logreturn1Month','logreturn3Month','logreturn6Month','logreturn12Month','returnStdOneMonth','meanTurnOneMonth','PE','PB','PS','PCF','profitGrowthTTM','revenueGrowthTTM','PEG','ROE']]
        #datSelect.rename(columns = {'Date':'Date', 'windcode':'windcode', 'industry_name':'industry_name','Volume':'volume','MV':'MV','logreturn1Month':'logreturn1Month','future_return':'future_return','future_logreturn':'future_logreturn','PE':'EP','PB':'BP'}, inplace = True)
        
        datSelect = datSelect.sort_values(by =['windcode','Date'], ascending = True)
      
        return datSelect
     
    
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
                perPeriod['Date'] =pd.to_datetime(perPeriod['Date'], format = '%Y%m%d')
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
    数据预处理函数，哪些预处理在数据库那边处理，哪些在模型里处理，需要确定
    1. 缺失值处理
    2. 极值处理
    3. 每期股票的预期收益率（月收益率）
    """
    def __dataPreProcess(self, perPeriod, factorName):
        """
        剔除掉成交量为0的交易日数据,删除停牌股票
        """
        perPeriod = perPeriod[perPeriod['Volume'] != 0]
        """
        perPeriod = perPeriod.dropna()   #此处dropna不能缺少，否则会导致回归失败
        """
        
        """
        删除因子值为Null的行
        """
        perPeriod = perPeriod.dropna(subset = [factorName])
     
        """
        if len(perPeriod) < 1000:   #原始输入数据存在几个数据量较小的月份数据，剔除
              print perPeriod.shape[0]
              return (False, perPeriod)
        """
                        
        """
        因子中位数去极值
        """
  
        medianValue = perPeriod[factorName].median()
        medianAbs = abs(perPeriod[factorName] - medianValue).median()
        perPeriod.ix[perPeriod[factorName] > medianValue+3*medianAbs, factorName] = medianValue+3*medianAbs
        perPeriod.ix[perPeriod[factorName] < medianValue-3*medianAbs, factorName] = medianValue-3*medianAbs
       
                   
        """
        因子标准化，标准化方式不同得出来的因子收益率相差较大
        """
        perPeriod[factorName] = (perPeriod[factorName] - perPeriod[factorName].mean())/perPeriod[factorName].std()
        #perPeriod[factorName] = (perPeriod[factorName] - perPeriod[factorName].min())/(perPeriod[factorName].max() - perPeriod[factorName].min())
    
        """
        新的因子暴露度序列缺失的地方设为0.视作存在缺失值时我们认为此个股因子值与全市场平均情况相同
        """
        perPeriod = perPeriod.fillna(0)
       
        """
        perPeriod = perPeriod.dropna()
        """
        
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
    def __WLSRegression(self, perPeriod, factorName):
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
        dummy = sm.categorical(perPeriod['industry_name'].values, drop = True)
        factorArray = np.column_stack((factorArray, dummy))  
                               
        """
        test，将MV为负数的部分置为0
        """
        perPeriod.ix[perPeriod['MV'] < 0, 'MV'] = 0
        
        """
        weightArray = np.sqrt(perPeriod['MV'].values)
        model3 = sm.WLS(logreturnArray, factorArray, weights = weightArray)
        
        """
 
        model3 = sm.WLS(logreturnArray, factorArray)
        results3 = model3.fit()
                        
        """
        model3 = smf.ols(formula='future_logreturn ~ %s+C(industry_name)'%(factorName), data=perPeriod,weights = weightArray )
        results3 = model3.fit()
        """
        
        return results3
    
    def __getICValue(self, perPeriod, factorName):
         """
         IC值计算，需要排除市值和行业对因子暴露度影响，
         以因子暴露度为因变量，对市值因子和行业因子做回归。
         用残差替代原来的暴露值，求残差与股票预期收益率的相关系数
         """
                        
         logreturnArray = perPeriod['future_logreturn'].values  
         """
         modelIC = smf.ols(formula='%s ~ MV+C(industry_name)'%(factorName), data=perPeriod)
         """
         
         modelIC = smf.ols(formula='%s ~ C(industry_name)'%(factorName), data=perPeriod)
         resultsIC = modelIC.fit()
         
         """
         获得残差值
         """
         ICValue = np.corrcoef(resultsIC.resid, logreturnArray)[0][1]
         return ICValue
                                
    
    def singleFactorRegression(self, factorList):
        
        datSelect = self.__selectDataColumns()
        datSelect = self.__getMonthlyData3(datSelect)
        
    
        """
        记录因子各项评价指标
        """
        IndexFactorList = pd.DataFrame(columns = ['因子名称','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
        IndexFactorOLSList = pd.DataFrame(columns = ['因子名称','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
        
        """
        按照每个截面分组回归
        """
        datPerPeriod = datSelect.groupby("Date")
        datPerPeriod_groups = datSelect.groupby("Date").groups
        
        """
        保存因子累积收益率
        """
        #cumReturn = pd.DataFrame(columns = factorList)
        cumReturn = pd.DataFrame()                                     
                                               
                                               
                                               
        """
        遍历所有因子
        """
        for factorName in factorList:  
            
            count = 0
            
            TvalueListOriginal = np.array([])
            returnListOriginal = np.array([])
            TvalueList = np.array([])
            returnList = np.array([])
            ICValueList = np.array([])
            
            timeList = np.array([])
            
            """
            遍历所有时间截面
            """
            
            print(factorName)
            for name, group in datPerPeriod:
                
                if name in datPerPeriod_groups:
                    perPeriod = datPerPeriod.get_group(name)          #获取一个截面的数据
                    
                    
                    #测试添加
                    print(count, name)
           
                                                      
                    
                    (ret, perPeriod) = self.__dataPreProcess(perPeriod, factorName)
                    
                    if ret == False:
                        print('skip once')
                        continue
                
                    results = self.__OlSRegression(perPeriod, factorName)
                    results3 = self.__WLSRegression(perPeriod, factorName)
                    ICValue = self.__getICValue(perPeriod, factorName)
                    
                    
                       
                    TvalueListOriginal = np.append(TvalueListOriginal, results.tvalues[0])
                    returnListOriginal = np.append(returnListOriginal, results.params[0])
                    TvalueList = np.append(TvalueList, results3.tvalues[0])
                    returnList = np.append(returnList, results3.params[0])
                    ICValueList = np.append(ICValueList, ICValue)
                    
                    """
                    时间截面记录
                    """
                    timeList = np.append(timeList, name)
                    
                    count +=1
                    
                    #print 'count is %d'%(count)
                  
         
            cumReturn[factorName] = returnList 
           
                        
      
            """
            获取|T|绝对值序列
            """
            TvalueListOriginal =np.nan_to_num(TvalueListOriginal)
            TvalueList = np.nan_to_num(TvalueList)
            ICValueList = np.nan_to_num(ICValueList)
            
            TvalueListOriginalAbs = np.fabs(TvalueListOriginal)
            TvalueListAbs = np.fabs(TvalueList)
            """
            T序列T检验值
            """
            stat_ols_val, p_ols_val = stats.ttest_1samp(TvalueListOriginal,0)
            stat_val, p_val = stats.ttest_1samp(TvalueList, 0)
         
            """
            WLS
            """
            
            print('Tvalue List')
            print(TvalueList)
            
            print('abs')
            print(TvalueListAbs.size)
            print(TvalueListAbs)
            
            print('IC List')
            print(ICValueList)
            
            df1 = pd.DataFrame([[factorName,TvalueListAbs.mean(),(TvalueListAbs[(TvalueListAbs > 2)].size)/(TvalueListAbs.size),TvalueList.mean(),TvalueList.mean()/TvalueList.std(),returnList.mean(),stat_val,ICValueList.mean(), ICValueList.std(),ICValueList[(ICValueList > 0)].size / ICValueList.size,ICValueList.mean()/ICValueList.std()]], columns = ['因子名称','|T|均值', '|T|>2占比', 'T均值','T均值/T标准差','因子收益率','T序列检验值','IC均值','IC标准差','IC>0比例','IR'])
            IndexFactorList = IndexFactorList.append(df1)
            
            """
            OLS
            """
            """
            df1 = pd.DataFrame([[factorName,TvalueListOriginalAbs.mean(),(TvalueListOriginalAbs[(TvalueListOriginalAbs > 2)].size)/(TvalueListOriginalAbs.size),TvalueListOriginal.mean(),TvalueListOriginal.mean()/TvalueListOriginal.std(),returnListOriginal.mean(),stat_ols_val,ICValueList.mean(), ICValueList.std(),ICValueList[(ICValueList > 0)].size / ICValueList.size,ICValueList.mean()/ICValueList.std()]], columns = [u'因子名称',u'|T|均值', u'|T|>2占比', u'T均值',u'T均值/T标准差',u'因子收益率',u'T序列检验值',u'IC均值',u'IC标准差',u'IC>0比例',u'IR'])
            IndexFactorOLSList = IndexFactorOLSList.append(df1)
            """
                        
        print(count)
        print('OLS result')
        print(IndexFactorOLSList)
        
        print('WLS result')
        print(IndexFactorList)
        
        """
        保存到csv文件
        """
        IndexFactorList.to_csv("dataFile/SingleFactorTest.csv", encoding = 'gbk')
        
        cumReturn = cumReturn.cumsum()
        cumReturn+=1
        
        cumReturn['Date'] = timeList
        cumReturn.set_index('Date', inplace = True)
        cumReturn.to_csv('dataFile/cumReturn.csv', encoding = 'gbk')
        
        
        
       
        
    """
    OLS的测试性代码
    Y = 1+10*X 与 X的线性回归
    """
    def linearRegressionTest(self):
        nsample = 100
        """
        x值从0到10的等差排列
        """
        x = np.linspace(0,10, nsample)
        """
        常数项1
        """
        X = sm.add_constant(x)
        """
        贝塔值设定
        """
        beta = np.array([1,10])
        """
        误差项设定
        """
        e = np.random.normal(size = nsample)
        """
        求得Y
        """
        y = np.dot(X, beta) + e
        model = sm.OLS(y,X)
        results = model.fit()
        print(results.params)
        print(results.tvalues)
        print(results.summary())
    


def main():
    
    
    SingleFactor = SingleFactorAnalyze()
    SingleFactor.readData()

    
    print('do single regression1')
    #factorList = ['MV', 'logreturn1Month','logreturn3Month','logreturn6Month', 'meanTurnOneMonth', 'meanTurnThreeMonth','meanTurnSixMonth','returnStdOneMonth','returnStdThreeMonth','returnStdSixMonth','maxToMinOneMonth','maxToMinThreeMonth','maxToMinSixMonth']
    #factorList = ['MV', 'logreturn1Month', 'meanTurnOneMonth', 'returnStdOneMonth','maxToMinOneMonth']
    #factorList = ['MV', 'logreturn1Month','returnStdOneMonth']
    
    #factorList = ['MV','logreturn1Month','logreturn3Month','logreturn6Month','logreturn12Month','returnStdOneMonth','meanTurnOneMonth','PE','PB','PS','PCF','profitGrowthTTM','revenueGrowthTTM','PEG','ROE']
    
    #factorList = ['PE','PB','PS','logreturn1Month','logreturn3Month','logreturn6Month','logreturn12Month','returnStdOneMonth','meanTurnOneMonth','PCF','profitGrowthTTM','revenueGrowthTTM','PEG','ROE']
    
    factorList = ['MV']
    
    SingleFactor.singleFactorRegression(factorList)
   
 

    
    
if __name__ == "__main__":
    main()
    
    
    
    
    
    
    
    
    
