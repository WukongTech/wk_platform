# -*- coding: utf-8 -*-
"""
Created on Mon Oct 09 16:41:32 2017

@author: cxd


说明�?
均线策略，每20个交易日调仓一次�?
卖出高于20均线的股票，买入低于20均线的股�?
"""


from __future__ import division  #执行浮点除法

"""
路径依赖的添加，使用命令行编译时需要路径依赖的支持
第二条，第三条为服务器上路径
"""
import sys
sys.path.append('C:\\platform\\pyalgotrade_cn_wk')
sys.path.append('/home/wk_thu_bkt_public/platform/pyalgotrade_cn_wk')
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
回测策略�?
"""
class MyStrategy(strategy.BacktestingStrategy):
    
    """
    参数说明�?
    feed：数据流feed类型
    instruments: 回测标的列表
    datFin: 传递给策略的财务指标数�?
    """
    def __init__(self, feed, instruments,datTotal, begin,end):
        
        """
        初始化策略父类，设定回测起始资金
        """
        strategy.BacktestingStrategy.__init__(self, feed,100000000)
        """
        佣金设置为万�?
        """
        self.getBroker().setCommission(TradePercentage(0.0003)) #设置佣金比例
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
        self.__price = datTotal
        
        """
        记录调仓日期
        """
        self.__buyDate = []
        self.__daysCount = 0
 
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
        每周一调仓，此处涉及到中美时区的差�?
        """
        if (bars.getDateTime().weekday() == 1):
            return True
        else:
            return False
        
    """
    N天调�?
    """
    def __ifTradeDayNdays(self, N):
        
        self.__daysCount +=1
        if self.__daysCount % N == 0:
            return True
        else:
            return False
    

     
    """
    均线函数
    """
    def MA(self,df, n):
        MA = pd.Series(pd.rolling_mean(df['Close'], n), name = 'MA_' + str(n))
        df = df.join(MA)
        return df    




    """
    策略开始运行时执行,此处计算�?0日均�?
    """
    def onStart(self):
        print('strategy start')

        """
        筛选需要用的数据，此处以一只股票的均线策略为例，筛�?00001.SZ的价格及成交量数�?
        """
        self.__price = self.__price[self.__price['windcode'] == '000001.SZ']
        self.__price = self.__price[['Date','windcode','Open', 'High','Low','Close','Volume']]
        print(self.__price.columns)
        print(self.__price.shape)
        
        """
        调用均线函数，求�?0日均线价�?
        """
        self.__price = self.MA(self.__price, 20)
        #print self.__price
    
    """
    策略运行结束时执�?
    """
    def onFinish(self, bars): 
        print('strategy stop')
        
        
    """
    每天的数据流到来时触发一�?
    """
    def onBars(self, bars):
        
        if self.__ifTradeDayNdays(20) == True:
            
            for inst in bars.getInstruments():
                pass
        else:
            pass
        
    
    def onBars(self, bars):
        
        if self.__currentYear != bars.getDateTime().year:
            self.__currentYear = bars.getDateTime().year
            print("current year is %s"%(self.__currentYear))
        
        
        """
        策略逻辑，判断当天开盘价�?0日均线价格的大小，超�?0均线则卖出，小于20均线则买�?
        """
        
        """
        获取当天日期
        """
        y = bars.getDateTime().year
        m = bars.getDateTime().month
        d = bars.getDateTime().day
        nowTime = datetime.datetime(y, m, d)
        nowTimeStr = nowTime.strftime("%Y%m%d")   #注意时间格式
        
                                     
        """
        取得当天价格数据
        """
        currentPrice = self.__price[self.__price['Date'] == nowTimeStr] 
        """
        均线策略�?0天没有均价数据，所以跳�?
        """
        currentPrice = currentPrice.dropna()
        if currentPrice.empty:
            return
        """
        重置行索�?
        """
        currentPrice = currentPrice.reset_index()
        print(currentPrice)
        

        
        
        inst = '000001.SZ'
        """
        获取当前持仓
        """
        shares = self.getBroker().getShares(inst)
        
        if  currentPrice.loc[0,'Open'] > currentPrice.loc[0,'MA_20']:
            print(nowTimeStr)
            print('less than MA_20, buy stocks')
            """
            当前没有持仓时，所有现金买入股�?
            """
            if shares == 0:
                self.enterLongCashAmount(bars, inst, self.getBroker().getCash(),False,False)
            
        elif currentPrice.loc[0,'Open'] < currentPrice.loc[0,'MA_20']:
            print(nowTimeStr)
            print('larger than MA_20, sell stocks')
            """
            当前有持仓时，卖光所有股�?
            """
            if shares > 0:
                self.enterShort(bars, inst, shares, False, False)
            
        
      
    
        
    
"""
将数据文件从HDF5读取到内存，保存为dataFrame格式
"""
def loadDataFromHDF5(begin, end):
 

    #datTotal = pd.read_hdf("C://platformData/priceAndFactorData/newlyPriceData_post.h5")
    """
    服务器路�?
    """
    datTotal = pd.read_hdf("/home/wk_thu_bkt_public/platformData/autoDailyData/priceDataForBackTest.h5")
    
    begin = str(begin)
    end = str(end)
    
    datTotal = datTotal[(datTotal['Date'] >=begin)&(datTotal['Date']<=end)]
    datTotal['Date'] =pd.to_datetime(datTotal['Date'], format = '%Y%m%d')
    datTotal['listdate'] =pd.to_datetime(datTotal['listdate'], format = '%Y%m%d')
    datTotal = datTotal[datTotal['st'] == 0]
    datTotal = datTotal[(datTotal['Date'] - datTotal['listdate']).dt.days > 180]
    datTotal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in datTotal['Date']]
    print("strategy read price data at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))

    
    return datTotal
   
 

def strategyProcess(datTotal,begin,end):
    
    
    #将读取出的股票按照windcode分组
    group = datTotal.groupby("windcode")           #按照windcode分类
    groups = datTotal.groupby("windcode").groups   #groups类型为dict
    
    """
    将dataframe格式的数据流读取到feed�?
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
    myStrategy = MyStrategy(feed, input_instruments,datTotal,begin,end)
    
    """
    策略运行及策略输�?
    """
    resultName =begin + "_" +end+"_"+"backtest"
    output = strategyOutput.StrategyOutput(myStrategy, begin, end)
    output.pre_process()
    myStrategy.run()
    output.bench_process()
    output.post_process(resultName)
    
    print("strategy ends at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))



if __name__ == "__main__":
    


    begindate = '20160901'
    enddate = '20170901'
    
    print('MA backtest')
    

    """
    将数据从HDF5文件读取到内�? 获取下个数据和处理完成的每期要买入股票列�?
    """
    datTotal = loadDataFromHDF5(begindate, enddate)
    strategyProcess(datTotal,begindate,enddate)
    
 
    
            
            
            
            
            
    
        
    
   

   
   
    
   
  
    
    
    

    

    


