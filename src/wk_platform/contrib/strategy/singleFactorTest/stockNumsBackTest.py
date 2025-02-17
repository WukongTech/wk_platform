# -*- coding: utf-8 -*-
"""
Created on Mon Oct 09 16:41:32 2017

@author: cxd


按照数量下单的策略回测函数
"""


from __future__ import division  #执行浮点除法

"""
路径依赖的添加，使用命令行编译时需要路径依赖的支持
第二条，第三条为服务器上路径
"""
import sys
sys.path.append('C:\\platform\\pyalgotrade_cn_wk')
sys.path.append('/home/chenxd/platform/pyalgotrade_cn_wk')
sys.path.append('/mnt/sdbdata/chenxd/platform/pyalgotrade_cn_wk')



import pandas as pd
import datetime
import time
import cProfile
import pstats
import multiprocessing
from pandas.tseries.offsets import Day, MonthEnd

from wk_platform import strategy
from wk_platform.dataframeFeed import data_frame_feed
from wk_platform.backtest import strategyOutput
from wk_platform.broker.backtesting import TradePercentage


"""
回测策略类
"""
class MyStrategy(strategy.BacktestingStrategy):
    
    """
    参数说明：
    feed：数据流feed类型
    instruments: 回测标的列表
    datFin: 传递给策略的财务指标数据
    """
    def __init__(self, feed, instruments,datFin, begin,end):
        
        """
        初始化策略父类，设定回测起始资金
        """
        strategy.BacktestingStrategy.__init__(self, feed,100000000)
        """
        佣金设置为千三
        """
        self.getBroker().setCommission(TradePercentage(0.003)) #设置佣金比例
        self.__instruments = instruments
        
        """
        记录策略开始和结束时间
        """
        self.__begindate = begin
        self.__enddate = end
        
        self.__currentYear = 0
        self.__currentMonth = 0
        
 
        
        """
        记录股票权重数据
        """
        self.__numsToBuy = datFin
        
        """
        记录调仓日期
        """
        self.__buyDate = []

    def getBuyDate(self):
        return self.__buyDate
        
    """
    判断当天是否是调仓日,此处设定月初为调仓日
    """
    def __ifTradeDayMonthly(self,bars):
      
        if (self.__currentMonth != bars.getDateTime().month):       
            self.__currentMonth = bars.getDateTime().month
            return True
   
        else:
            return False
        
    """
    按周调仓
    """
    def __ifTradeDayWeekly(self,bars):
        """
        每周一调仓，此处涉及到中美时区的差别
        """
        if (bars.getDateTime().weekday() == 1):
            return True
        else:
            return False
    
   
 
    """
    获取当前调仓日选取出来的持仓股票列表
    """
    def __getToBuyListVAL(self,bars):
       pass
      
    def __sellStock(self,bars):  
       pass
    def __buyStock(self,bars): 
       pass
   
    
    """
    每期按照权重调仓
    """
    def __changePosition(self, bars):
        
      numsToBuyPerTrade = self.__numsToBuy[self.__numsToBuy["windcode"].isin(bars.getInstruments())]
      
      y = bars.getDateTime().year
      m = bars.getDateTime().month
      d = bars.getDateTime().day
      newTime = datetime.datetime(y, m, d)
      newTimeStr = newTime.strftime("%Y%m%d")   #注意时间格式
      
      """
      取得调仓截面的数据
      """
      numsToBuyPerTrade = numsToBuyPerTrade[numsToBuyPerTrade['Date'] == newTimeStr]   
      numsToBuyPerTrade = numsToBuyPerTrade.set_index('Date')
      
      """
      取得调仓截面的股票代码列表以及交易数量
      """
      windcodeList = numsToBuyPerTrade['windcode'].tolist()
      numsList = numsToBuyPerTrade['num'].tolist()
      
      print(len(windcodeList))
      for (inst, nums) in zip(windcodeList, numsList):
          
          if nums > 0:
              self.enterLong(bars, inst, nums,False, False)
          elif nums < 0:
              self.enterShort(bars, inst, nums, False, False)
      
      
    
    

    """
    策略开始运行时执行
    """
    def onStart(self):
        """
        获取调仓日期
        """
        self.__buyDate = list(set(self.__numsToBuy['Date']))
        print(self.__buyDate)
        print(len(self.__buyDate))
    """
    策略运行结束时执行
    """
    def onFinish(self, bars): 
        pass
        
    """
    每天的数据流到来时触发一次
    """
    def onBars(self, bars):
        if self.__currentYear != bars.getDateTime().year:
            self.__currentYear = bars.getDateTime().year
            print("current year is %s"%(self.__currentYear))
     
        """
        判断当前时间是否是调仓日
        """
        y = bars.getDateTime().year
        m = bars.getDateTime().month
        d = bars.getDateTime().day
        newTime = datetime.datetime(y, m, d)
        newTimeStr = newTime.strftime("%Y%m%d")   #注意时间格式
        
        """
        筛选出调仓日列表
        """
        if newTimeStr in self.__buyDate:
            print(newTimeStr)
            self.__changePosition(bars)
        
        

    
"""
将数据文件从HDF5读取到内存，保存为dataFrame格式
"""
def loadDataFromHDF5(begin, end):

    datTotal = pd.read_hdf("data/priceDataForBackTestNew.h5")
    """
    服务器路径
    """
    #datTotal = pd.read_hdf("/home/chenxd/platformData/priceAndFactorData/newlyPriceData_post.h5")
    begin = str(begin)
    end = str(end)
    
    datTotal = datTotal[(datTotal['Date'] >=begin)&(datTotal['Date']<=end)]
    datTotal['Date'] =pd.to_datetime(datTotal['Date'], format = '%Y%m%d')
    datTotal['listdate'] =pd.to_datetime(datTotal['listdate'], format = '%Y%m%d')
    datTotal = datTotal[datTotal['st'] == 0]
    datTotal = datTotal[(datTotal['Date'] - datTotal['listdate']).dt.days > 180]
    datTotal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in datTotal['Date']]
    print("strategy read price data at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))

    #datVal = pd.read_hdf("C://platformData/priceAndFactorData/dataForRegressionPost.h5")
    """
    读取每期交易清单列表
    """
    datVal = pd.read_csv("/home/wk_thu_bkt_public/platform/pyalgotrade_cn_wk/wukongPlatform/wkUserCode/strategyBackTestDemo/dataFile/000001.SZ.trade3.csv", encoding = 'gbk')
    datVal['Date'] =pd.to_datetime(datVal['Date'], format = '%Y/%m/%d')
    datVal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in datVal['Date']]
    datVal = datVal[(datVal['Date'] >=begin)&(datVal['Date']<=end)]

    

    print("strategy read finance data at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))

    
    return (datTotal,datVal)
   
 

def strategyProcess(datTotal, datFin,begin,end):
    
    
    #将读取出的股票按照windcode分组
    group = datTotal.groupby("windcode")           #按照windcode分类
    groups = datTotal.groupby("windcode").groups   #groups类型为dict
    
    """
    将dataframe格式的数据流读取到feed中
    """
    count = 0
    feed = dataFramefeed.Feed()
    datNew = pd.DataFrame()
    input_instruments = []
    
    
    for inst in groups.keys():
        count+=1
        datNew = group.get_group(str(inst)).sort_values(by='Date')
        """
        此处对日期做去重处理
        """
        datNew = datNew.drop_duplicates('Date')  #去重
        datNew = datNew.set_index('Date')        #将date设为索引
        feed.addBarsFromDataFrame(str(inst), datNew)   #将dataframe格式加载进windfeed
        input_instruments.append(inst)
        
    print('total instrument load is %d' %(count))
    print("strategy dataframeFeed load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))
    
    
    
    """
    实例化策略类
    """
    myStrategy = MyStrategy(feed, input_instruments,datFin,begin,end)
    
    """
    策略运行及策略输出
    """
    resultName =begin + "_" +end+"_"+"backtest"
    output = strategyOutput.StrategyOutput(myStrategy, begin, end)
    output.pre_process()
    myStrategy.run()
    output.bench_process()
    output.post_process(resultName)
    
    print("strategy ends at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))



if __name__ == "__main__":
    


    begindate = '20050901'
    #enddate = '20160901'
    enddate = '20051201'
    
    print('weight backtest')
    

    """
    将数据从HDF5文件读取到内存, 获取下个数据和处理完成的每期要买入股票列表
    """
    (datTotal, datVal) = loadDataFromHDF5(begindate, enddate)
    strategyProcess(datTotal,datVal,begindate,enddate)
 
    
            
            
            
            
            
    
        
    
   

   
   
    
   
  
    
    
    

    

    


