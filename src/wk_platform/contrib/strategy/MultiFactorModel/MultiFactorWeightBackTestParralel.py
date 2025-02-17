# -*- coding: utf-8 -*-
"""
Created on Mon Oct 09 16:41:32 2017

@author: cxd
"""
  #执行浮点除法
"""
路径依赖的添加，使用命令行编译时需要路径依赖的支持
第二条，第三条为服务器上路径
"""
import sys
import os
sys.path.append('C:\\platform\\pyalgotrade_cn_wk')
sys.path.append('/home/wk_thu_bkt_public/platform/pyalgotrade_cn_wk')
#sys.path.append('/mnt/sdbdata/chenxd/platform/pyalgotrade_cn_wk')


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
from wk_platform.broker.backtesting import TradePercentageTaxFee
#from wk_platform.broker import slippage


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
    def __init__(self, feed,datVal, begin,end):
        
        """
        初始化策略父类，设定回测起始资金
        """
        strategy.BacktestingStrategy.__init__(self, feed,10000000)
        """
        佣金设置为万三
        """
        self.getBroker().setCommission(TradePercentage(0.0003))
        """
        设置印花税为千一
        """
        self.getBroker().setCommissionTaxFee(TradePercentageTaxFee(0.001))
        
        """
        self.__instruments = instruments
        """
        
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
        self.__weight = datVal
        
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
    
      weightPerTrade = self.__weight[self.__weight["windcode"].isin(bars.getInstruments())]
      y = bars.getDateTime().year
      m = bars.getDateTime().month
      d = bars.getDateTime().day
      newTime = datetime.datetime(y, m, d)
      newTimeStr = newTime.strftime("%Y%m%d")   #注意时间格式
      weightPerTrade = weightPerTrade[weightPerTrade['Date'] == newTimeStr]   
      weightPerTrade = weightPerTrade.set_index('Date')
   
      """
      获取了当期要买入的股票列表和权重列表
      按照权重从大到小的顺序买入，先买优先股
      """
      weightPerTrade = weightPerTrade.sort_values(by = 'weight', ascending = False)
      
      """
      对于权重小于0.01的股票做丢弃处理
      """
      #weightPerTrade = weightPerTrade[weightPerTrade['weight'] >= 0.01]
      windcodeList = weightPerTrade['windcode'].tolist()
      weightList = weightPerTrade['weight'].tolist()
      """
      print 'the length is'
      print len(weightList)
      #print windcodeList
      """
      """
      先卖后买的顺序，对于存在于上一期持仓，但是不存在当期持仓的股票清仓卖出
      """
      if len(self.getBroker().getPositions()) > 0:
          for inst in list(self.getBroker().getPositions().keys()):
              if inst not in windcodeList:
                  shares = self.getBroker().getShares(inst)
                  if shares > 0:
                      #print 'sell out %s'%(inst)
                      self.enterShort(bars, inst, shares, False, False)
      
      """
      对于本期要买入的股票，按照权重买入
      """
      for (inst, weight) in zip(windcodeList, weightList):
          #print inst, weight
          self.enterLongShortWeight(bars, inst, weight,False, False)
      

    """
    策略开始运行时执行
    """
    def onStart(self):
        """
        获取调仓日期
        """
        self.__buyDate = list(set(self.__weight['Date']))
        print('buy Date is\n')
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
        if newTimeStr in self.__buyDate:
            print(newTimeStr)
            self.__changePosition(bars)
        

"""
将数据文件从HDF5读取到内存，保存为dataFrame格式
"""
def loadDataFromHDF5(begin, end, localOrServer):
 
    if localOrServer == 0:
        datTotal = pd.read_hdf("C://platformData/autoDailyData/priceDataForBackTest.h5")
        #datTotal = pd.read_hdf("C://platformData/autoDailyData/newlyPriceData_post.h5")
    else:
        datTotal = pd.read_hdf("/home/wk_thu_bkt_public/platformData/autoDailyData/priceDataForBackTest.h5") 
    begin = str(begin)
    end = str(end)
    datTotal = datTotal[(datTotal['Date'] >=begin)&(datTotal['Date']<=end)]
    print(("strategy read price data at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
    return datTotal

"""
将数据文件解析为回测平台能识别的数据流
"""
def strategyProcess_pre(datTotal,begin,end):
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
    for inst in list(groups.keys()):
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
    print(("strategy dataframeFeed load at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
    
    return feed
     
"""
读取用于回测的持仓标的文件
"""
def strategyProcess_post(feed,fileName,begin,end):
   
    """
    实例化策略类
    """
    datVal = pd.read_csv('dataFile/weightFile/weightFileFinalOutput/%s'%(fileName), encoding = 'gbk')
    datVal['Date'] =pd.to_datetime(datVal['Date'], format = '%Y/%m/%d')
    datVal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in datVal['Date']]
    datVal = datVal[(datVal['Date'] >=begin)&(datVal['Date']<=end)]
    
    myStrategy = MyStrategy(feed,datVal,begin,end)
    """
    策略运行及策略输出
    """
    resultName =begin + "_" +end+"_"+"backtest"+"_"+fileName
    #resultName = fileName
    
    """
    读取benchMark文件
    """
    userBenchMark = pd.read_csv("benchmarkself.csv", encoding = 'gbk')
    userBenchMark['日期'] =pd.to_datetime(userBenchMark['日期'], format = '%Y/%m/%d')
    userBenchMark['日期'] = [pd.datetime.strftime(x, '%Y-%m-%d') for x in userBenchMark['日期']]
    #print userBenchMark
    output = strategyOutput.StrategyOutput(myStrategy, begin, end, '300增强策略', 0.02, userBenchMark)
    #output = strategyOutput.straOutput(myStrategy,begin,end)
    output.pre_process()
    myStrategy.run()
    output.bench_process()
    output.post_process(resultName)
    print(("strategy ends at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))



if __name__ == "__main__":
    
    localOrServer = 1
    begindate = '20090101'
    #begindate = '20170901'
    enddate = '20180125'
    #enddate = '20170601'
    print('multifactor weight backtest, including slippage, all stocks after 2009 to 20180125,300 begin test')
    
    
    """
    将数据从HDF5文件读取到内存, 获取下个数据和处理完成的每期要买入股票列表
    """
    print(("backTest begins at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))
    datTotal = loadDataFromHDF5(begindate, enddate, localOrServer)
    
    """
    遍历文件夹下的所有文件
    """
    for root, dirs, files in os.walk("dataFile/weightFile/weightFileFinalOutput"):
        print(root)
        print(dirs)
        print(files)
        fileList = files

    print('files List')
    print(fileList)
    for fileName in fileList:
        print(fileName)
        feed = strategyProcess_pre(datTotal,begindate,enddate)
        strategyProcess_post(feed,fileName,begindate,enddate)
        
    print(("backTest ends at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))

   
            
            
            
            
            
    
        
    
   

   
   
    
   
  
    
    
    

    

    


