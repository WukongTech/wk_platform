# -*- coding: utf-8 -*-
import sys
import datetime
import time

import pandas as pd

from wk_util.configuration import Configuration
from wk_platform import strategy
from wk_platform.dataframeFeed import data_frame_feed
from wk_platform.backtest import strategyOutput
from wk_platform.broker.backtesting import TradePercentage
from wk_platform.broker.backtesting import TradePercentageTaxFee


class MyStrategy(strategy.BacktestingStrategy):
    """
    回测策略类
    """
    
    """
    参数说明：
    feed：数据流feed类型
    instruments: 回测标的列表
    datFin: 传递给策略的财务指标数据
    """
    def __init__(self, feed, instruments, datFin, begin,end):
        """
        @param

        """
        
        """
        初始化策略父类，设定回测起始资金
        """
        strategy.BacktestingStrategy.__init__(self, feed, 100000000)
        """
        佣金设置为万三
        """
        self.getBroker().setCommission(TradePercentage(0.0003)) #设置佣金比例
        """
        设置印花税为千一
        """
        self.getBroker().setCommissionTaxFee(TradePercentageTaxFee(0.001))
          
                      
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
    判断当天是否是调仓日，此处设定月初为调仓日
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
    策略开始运行时执行
    """
    def onStart(self):
        pass
        
    
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
            print("current year is %s" % (self.__currentYear))
     
        """
        判断当前时间是否是调仓日
        """
        y = bars.getDateTime().year
        m = bars.getDateTime().month
        d = bars.getDateTime().day
        newTime = datetime.datetime(y, m, d)
        newTimeStr = newTime.strftime("%Y%m%d")   #注意时间格式
        """
        print type(bars.getDateTime())
        print bars.getDateTime()
        """
                                     
        inst = '000001.SZ'
        if newTimeStr == self.__begindate:
            print(newTimeStr)
            print('buy stocks')
            self.enterLongCashAmount(bars, inst, self.getBroker().getCash(), False, False)
        elif newTimeStr == self.__enddate:
            pass
        else:
            pass


def loadDataFromHDF5(begin, end):
    """
    将数据文件从HDF5读取到内存，保存为dataFrame格式

    Parameters
    ----------
    begin: int
        起始日期
    end: int
        结束日期

    Returns
    -------
    datTotal, datVal: (pd.DataFrame, pd.DataFrame)
        返回全部数据和经过筛选的数据
    """
    datTotal = pd.read_hdf("../~data/priceDataForBackTestNew.h5")

    begin = str(begin)
    end = str(end)
    
    datTotal = datTotal[(datTotal['Date'] >=begin)&(datTotal['Date']<=end)]
    datTotal['Date'] =pd.to_datetime(datTotal['Date'], format = '%Y%m%d')
    datTotal['listdate'] =pd.to_datetime(datTotal['listdate'], format = '%Y%m%d')
    datTotal = datTotal[datTotal['st'] == 0]
    datTotal = datTotal[(datTotal['Date'] - datTotal['listdate']).dt.days > 180]
    datTotal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in datTotal['Date']]
    print(("strategy read price data at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))

    
    #datVal = pd.read_hdf("C://platformData/priceAndFactorData/dataForRegressionPost.h5")
    """
    读取每期权重列表
    """
    datVal = pd.read_csv("../~data/000001.SZ.trade3.csv", encoding ='gbk')
    datVal['Date'] =pd.to_datetime(datVal['Date'], format = '%Y/%m/%d')
    datVal['Date'] = [pd.datetime.strftime(x, '%Y%m%d') for x in datVal['Date']]
    datVal = datVal[(datVal['Date'] >=begin)&(datVal['Date']<=end)]
    

    print(("strategy read finance data at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))

    
    return datTotal, datVal
   
 

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

    for inst in list(groups.keys()):
        count += 1
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
    

    """
    实例化策略类
    """
    myStrategy = MyStrategy(feed, input_instruments, datFin, begin, end)
    
    """
    策略运行及策略输出
    """
    resultName =begin + "_" +end+"_"+"backtest"
    output = strategyOutput.StrategyOutput(myStrategy, begin, end)
    output.pre_process()
    myStrategy.run()
    output.bench_process()
    output.post_process(resultName)
    
    print(("strategy ends at %s" %(time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time())))))


if __name__ == "__main__":
    start_time = time.time()
    config = DataConfiguration()
    """
    DataConfiguration 说明
    第一个参数指定 benchFile 文件所在路径
    第二个参数指定 结果保存路径，请确认该目录已经创建好
    """
    print(config.bench_dir)

    # print("job starts at %s" % (time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))))
    

    
    begindate = '20161107'
    enddate = '20170822'
    
    """
    begindate = '20020101'
    enddate = '20050101'
    """

    
    print('buy and hold backtest')
    (datTotal, datVal) = loadDataFromHDF5(begindate, enddate)
    strategyProcess(datTotal, datVal, begindate, enddate)

    
    
    #getDataForRegression()

    end_time = time.time()
    print(f"job cost {end_time - start_time}s")
 
    
            
            
            
            
            
    
        
    
   

   
   
    
   
  
    
    
    

    

    


