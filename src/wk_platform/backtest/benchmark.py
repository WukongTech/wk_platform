# -*- coding: utf-8 -*-
"""
Created on Wed Jul 05 10:16:41 2017

@author: cxd

comment: 股市基准指数分析模块
"""
from __future__ import annotations

import datetime
import math
import pathlib
from functools import partial

import numpy as np
import pandas as pd

from wk_platform.math import stats
from wk_platform.config import StrategyConfiguration, PriceType, CalendarType
from wk_data import BenchDataSource
import wk_platform.backtest.util as bench_util
import wk_data
from wk_platform.util.data import align_calendar

INDEX_NAME_MAPPING = {
    "000001.SH": "上证综指",
    "000016.SH": "上证50",
    "000300.SH": "沪深300",
    "399905.SZ": "中证500",
    "000906.SH": "中证800",
    "000852.SH": "中证1000",
    "399006.SZ": "创业板指",
    "932000.CSI": "中证2000"
}


def tmp_user_benchmark(begin_date, end_date=None):
    index_mapping = {
        '518880.SH': '黄金ETF',
        'NDX.GI': '纳斯达克100',
        'SPX.GI': '标普500'
    }
    calendar = pd.DataFrame({
        'trade_dt': wk_data.get('trade_calendar', begin_date=begin_date, end_date=end_date)
    })
    df_list = []
    for k, v in index_mapping.items():
        data = wk_data.get('local_index', begin_date=begin_date, end_date=end_date, instrument=k)
        data = calendar.merge(data[['trade_dt', 'close']], on='trade_dt', how='left')
        data['close'] = data['close'].bfill()
        data.rename(columns={'close': v}, inplace=True)
        df_list.append(data)

    data = calendar.merge(df_list[0], on='trade_dt', how='left')
    data = data.merge(df_list[1], on='trade_dt', how='left')
    data = data.merge(df_list[2], on='trade_dt', how='left')

    data.rename(columns={'trade_dt': '日期'}, inplace=True)

    return data




class BenchmarkAnalyzer:
    
    def __init__(self, begin_date, end_date, user_benchmark=None, config=StrategyConfiguration()):
        
        self.__beginDate = begin_date
        self.__endDate = end_date
        self.__config = config
        
        self.__benchIndex = pd.DataFrame()
        """
        指数原始净值列表
        """
        self.__benchCombine: pd.DataFrame | None = None

        self.__indexList = [
            "000001.SH", "000016.SH", "000300.SH", "399905.SZ",
            "000852.SH", "399006.SZ", "000906.SH", "932000.CSI"
        ]
        # self.__indexList = ["000300.SH", "399905.SZ", "000906.SH", "000016.SH",  "000852.SH", "399006.SZ"]

        self.__dictIndexList = INDEX_NAME_MAPPING
        # self.__nameList = ['上证综指', '上证50', '沪深300', '中证500', '中证1000', '创业板指']
        self.__name_list = [INDEX_NAME_MAPPING[name] for name in self.__indexList]

        """
        经过归一化处理的指数净值列表
        """
        # self.__benchStandard = pd.DataFrame(columns=['日期', '上证综指', '上证50', '沪深300', '中证500', '中证1000', '创业板指'])
        self.__benchStandard = pd.DataFrame(columns=['日期'] + self.__name_list)

        # self.__hedgeList = ['策略对冲上证综指', '策略对冲上证50', '策略对冲沪深300', '策略对冲中证500', '策略对冲中证1000', '策略对冲创业板指']
        self.__hedge_list = ['策略对冲' + INDEX_NAME_MAPPING[name] for name in self.__indexList]

        # 记录用户自定义benchmark，20171228
        self.__user_benchmark = user_benchmark
        self.__user_bench_name_list = []
        self.__user_bench_hedge_list = []

        # assert user_benchmark is None or user_benchmark.empty, '临时禁用自定义基准'
        # self.__user_benchmark = tmp_user_benchmark(begin_date, end_date)

        if self.__user_benchmark is not None and self.__user_benchmark.empty == False:
            self.__user_benchmark = self.__user_benchmark.set_index('日期')
            for bench in self.__user_benchmark.columns:
                self.__user_bench_name_list.append(bench)
                hedge_name = '策略对冲' + bench
                self.__user_bench_hedge_list.append(hedge_name)
            self.__user_benchmark = self.__user_benchmark.reset_index()
        else:
            self.__user_benchmark = pd.DataFrame()
        
        
        """
        收益率指标
        """
        self.__cumulativeReturn = {}
        self.__annualReturn = {}
        
        """
        最大回撤指标
        """
        self.__maxdrawdown = {}
        self.__maxdrawdownTime = {}
        self.__maxdrawdownHigh = {}
        self.__maxdrawdownLow = {}
        
        """
        波动率及夏普比率指标
        """
        self.__volatility = {}
        self.__sharpeRatio = {}
        self.__daysBetween = {}
        
        """
        每年交易日
        """
        self.__tradeDaysOneYear = 365
        
    def getDictIndexList(self):
        return self.__dictIndexList
    
    def getIndexList(self):
        return self.__indexList
    
    def getNameList(self):
        return self.__name_list
    
    def getHedgeList(self):
        return self.__hedge_list

    def getFullList(self):
        return [self.__config.strategy_name] + self.getAllBenchList() + self.getAllHedgeList()
    
    """
    返回用户自定义基准， 20171228
    """
    def getUserBenchList(self):
        return self.__user_bench_name_list
    
    def getUserHedgeList(self):
        return self.__user_bench_hedge_list
    
    def getAllBenchList(self):
        return self.__name_list + self.__user_bench_name_list
    
    def getAllHedgeList(self):
        return self.__hedge_list + self.__user_bench_hedge_list
        
    
    """
    获取时间间隔
    """
    def getDaysBetween(self, begin, end):
        delta = abs(end - begin)
        ret = delta.days + 1
        return ret
        

    def getCumulativeReturn(self):
        return self.__cumulativeReturn
    def getAnnualReturn(self):
        return self.__annualReturn
    def getMaxDrawdown(self):
        return self.__maxdrawdown
    def getMaxDrawdowTime(self):
        return self.__maxdrawdownTime
    def getMaxDrawdownHigh(self):
        return self.__maxdrawdownHigh
    def getMaxDrawdownLow(self):
        return self.__maxdrawdownLow
    def getVolatility(self):
        return self.__volatility
    def getSharpeRatio(self):
        return self.__sharpeRatio

    def calculateReturn(self, strategyName):
        """
        计算收益率和年化收益率
        """
        # 计算天数间隔
        daysTotal = self.getDaysBetween(pd.to_datetime(self.__benchStandard.index[0]), pd.to_datetime(self.__benchStandard.index[-1]))
        
       
        #totalList = [strategyName] + self.__nameList + self.__hedgeList
        """
        新增用户自定义基准, 20171228
        """
        totalList = [strategyName] + self.__name_list + self.__user_bench_name_list + self.__hedge_list + self.__user_bench_hedge_list
        #for symbol in self.__nameList:
        for symbol in totalList:
            # self.__cumulativeReturn[symbol] = (self.__benchStandard[symbol].iloc[-1] - self.__benchStandard[symbol].iloc[0]) * 1.0 / self.__benchStandard[symbol].iloc[0]
            self.__cumulativeReturn[symbol] = self.__benchStandard[symbol].iloc[-1] - 1
            """
            self.__annualReturn[symbol] = self.__cumulativeReturn[symbol]/(daysTotal*1.0/self.__tradeDaysOneYear)
            """
            """
            此处需要使用复利进行计算
            """
            self.__annualReturn[symbol] = math.pow(self.__cumulativeReturn[symbol] + 1, self.__tradeDaysOneYear * 1.0 / daysTotal) - 1

    def calculateMaxdrawdown(self,strategyName):
        """
        计算最大回撤相关信息
        """
     
        #totalList = [strategyName] + self.__nameList + self.__hedgeList
        """
        新增用户自定义基准,20171228
        """
        totalList = [strategyName] + self.__name_list + self.__user_bench_name_list + self.__hedge_list + self.__user_bench_hedge_list
                    
        for symbol in totalList:
            highValue = 1
            highTime = 0
            lowTime = 0
            maxdd = 0

            maxHighTime = 0
            # maxHighValue = 0
            # maxLowValue = 0

            for index, row in self.__benchStandard[[symbol]].iterrows():
                #print index, row['Close']
                if row[symbol] > highValue:
                    highValue = row[symbol]
                    highTime = index
                else:
                    drawdownRet = (highValue - row[symbol]) / highValue
                    
                    if drawdownRet > maxdd:
                        maxdd = drawdownRet
                        lowTime = index
                        # maxLowValue = row[symbol]
                        #
                        # maxHighValue = highValue
                        maxHighTime = highTime

            
            self.__maxdrawdown[symbol] = maxdd

            # 时间间隔需要-1
            self.__maxdrawdownTime[symbol] = self.getDaysBetween(pd.to_datetime(lowTime), pd.to_datetime(maxHighTime)) - 1
            self.__maxdrawdownHigh[symbol] = maxHighTime
            self.__maxdrawdownLow[symbol] = lowTime
                        

    def calculateSharpeRatio(self, strategyName, riskFreeRate=0.05):
        """
        计算波动率和sharpe比率,无风险利率默认值为0.05
        """

        # 新增用户自定义基准,20171228
        totalList = [strategyName] + self.__name_list + self.__user_bench_name_list + self.__hedge_list + self.__user_bench_hedge_list

        for symbol in totalList:
            net_value = self.__benchStandard[symbol].tolist()
            if self.__config.price_type != PriceType.CLOSE:
                net_value = [1] + net_value
            net_value = pd.Series(net_value)
            ret_list = (net_value.diff(1) / net_value.shift(1)).dropna()
            # returnList = []
            #
            # lastValue = None
            # for index, row in self.__benchStandard[[symbol]].iterrows():
            #
            #     if lastValue != None:
            #         #returnsTracker[index][symbol] = row[symbol]/lastValue
            #         returnRatio = (row[symbol] - lastValue)/float(lastValue)
            #         returnList.append(returnRatio)
            #     lastValue = row[symbol]
                
            """
            需要检查
            年化波动率按照交易日数量计算更合理，改为250 2017/10/25
            """
            # self.__volatility[symbol] = ((self.__benchStandard.ix[:,[symbol]]).std()) * 0.01
            self.__volatility[symbol] = stats.stddev(ret_list, 1)
            self.__volatility[symbol] = float(math.sqrt(250) * self.__volatility[symbol])

            if self.__volatility[symbol] != 0:
                self.__sharpeRatio[symbol] = (self.__annualReturn[symbol] - riskFreeRate) / self.__volatility[symbol]
            else:
                self.__sharpeRatio[symbol] = np.nan

    def __prepare_index_bench(self):
        """
        构造指数的benchmark
        """
        # bench_source = BenchDataSource()
        for symbol in self.__indexList:

            # data = bench_source.get_daily(symbol, self.__beginDate, self.__endDate)
            data = wk_data.get('index_market', instrument=symbol, begin_date=self.__beginDate, end_date=self.__endDate)
            calendar = wk_data.get('trade_calendar', begin_date=self.__beginDate, end_date=self.__endDate,
                                       calendar=self.__config.calendar)
            align_func = partial(align_calendar, calendar=calendar)

            data = align_func(data)
            if data.empty:
                bench_combine_temp = pd.DataFrame({
                    "trade_dt": calendar,
                    "close": [1] * len(calendar)
                })
                open_price = 1
            else:
                # 读取时间列和Close列
                bench_combine_temp = data[["trade_dt", "close"]].copy()
                open_price = data.head(1)['open'].tolist()[0]
                if np.isnan(open_price):
                    bench_combine_temp = data[["trade_dt", "close"]].copy()
                    bench_combine_temp = bench_combine_temp.bfill()
                    open_price = bench_combine_temp.head(1)['close'].tolist()[0]

            bench_combine_temp['close'] = bench_combine_temp['close'] / open_price
            bench_combine_temp = bench_combine_temp.rename(columns={"close": symbol})
            if self.__benchCombine is None:
                self.__benchCombine = bench_combine_temp.reset_index(drop=True)
            else:
                self.__benchCombine = self.__benchCombine.merge(bench_combine_temp, on='trade_dt', how='outer')

        # 对于超出指数时间范围的点数，按照刚上市时的点数填充，目前假定不出现此种情况
        # self.__benchCombine = self.__benchCombine.fillna(method="bfill")
        self.__benchCombine = self.__benchCombine.bfill()

    def prepare_benchmark(self, strategy_equity, strategy_name='myStrategy'):
        """
        生成benchmark数据，会将策略和参考指标全部进行归一化

        Parameters
        -----------
        strategy_equity: pd.DataFrame
            策略的数据表

        strategy_name: str
            策略名称
        """
        self.__prepare_index_bench()

        # 根据时间范围限制进行截取 chenxiangdong 20170721 数据的时间格式可能存在差异
        bTemp = datetime.datetime.strptime(self.__beginDate, '%Y%m%d')
        begin = bTemp.strftime('%Y-%m-%d')
        ETemp = datetime.datetime.strptime(self.__endDate, '%Y%m%d')
        end = ETemp.strftime('%Y-%m-%d')

        if not self.__user_benchmark.empty:
            self.__user_benchmark = self.__user_benchmark[
                (self.__user_benchmark['日期'] >= self.__beginDate) & (self.__user_benchmark['日期'] <= self.__endDate)]
            self.__user_benchmark = self.__user_benchmark.bfill()

            # 合并指数和自定义基准
            self.__benchCombine = self.__benchCombine.merge(self.__user_benchmark, left_on="trade_dt", right_on="日期", how="inner")
            self.__benchCombine = self.__benchCombine.drop(columns=['日期'])

        strategy_equity['trade_dt'] = strategy_equity['日期'].apply(lambda s: s.replace('-', ''))
        strategy_equity = strategy_equity[
            (strategy_equity['trade_dt'] >= self.__beginDate) & (strategy_equity['trade_dt'] <= self.__endDate)]

        strategy_equity = strategy_equity.rename(columns={"总资产": strategy_name})[['trade_dt', strategy_name]]
        # 使用初始资金作为归一化基准，另可考虑第一日收盘净资产作为归一化基准。目前指数采用第一日收盘作为归一化基准
        strategy_equity[strategy_name] = strategy_equity[strategy_name] / self.__config.initial_cash

        # self.__benchCombine = strategy_equity.merge(self.__benchCombine, on="Date", how="inner")
        # self.__benchCombine = self.__benchCombine.rename(columns={"Date": "date"})

        self.__benchCombine = self.__benchCombine.rename(columns=self.__dictIndexList).set_index('trade_dt')

        # 使用收盘价时，用第一个交易日的收盘价进行归一化，否则使用第一个交易日的看盘价进行归一化（默认）
        if self.__config.price_type == PriceType.CLOSE:
            self.__benchCombine = (self.__benchCombine / self.__benchCombine.iloc[0, :]).reset_index()

        self.__benchStandard = strategy_equity.merge(self.__benchCombine, on="trade_dt", how="inner")
        self.__benchStandard = self.__benchStandard.rename(columns={"trade_dt": "date"}).set_index('date')

        bench_change = bench_util.add_net_value_base(self.__benchStandard)
        bench_change = bench_change.pct_change()
        bench_change = bench_util.remove_net_value_base(bench_change)


        # benchStandardChange = self.__benchStandard.pct_change()
        # benchStandardChange = benchStandardChange.fillna(0)
        # tempDataframe = pd.DataFrame()

        exceed_return_ratio = pd.DataFrame()

        # 计算对冲后净值
        exceed_return_ratio['策略超额上证综指比率'] = bench_change[strategy_name] - bench_change['上证综指'] + 1
        exceed_return_ratio['策略超额上证50比率'] =  bench_change[strategy_name] - bench_change['上证50'] + 1
        exceed_return_ratio['策略超额沪深300比率'] = bench_change[strategy_name] - bench_change['沪深300'] + 1
        exceed_return_ratio['策略超额中证500比率'] = bench_change[strategy_name] - bench_change['中证500'] + 1
        exceed_return_ratio['策略超额中证1000比率'] = bench_change[strategy_name] - bench_change['中证1000'] + 1
        exceed_return_ratio['策略超额创业板指比率'] = bench_change[strategy_name] - bench_change['创业板指'] + 1
        exceed_return_ratio['策略超额中证800比率'] = bench_change[strategy_name] - bench_change['中证800'] + 1
        exceed_return_ratio['策略超额中证2000比率'] = bench_change[strategy_name] - bench_change['中证2000'] + 1
        self.__benchStandard['策略对冲上证综指'] = exceed_return_ratio['策略超额上证综指比率'].cumprod()
        self.__benchStandard['策略对冲上证50'] = exceed_return_ratio['策略超额上证50比率'].cumprod()
        self.__benchStandard['策略对冲沪深300'] = exceed_return_ratio['策略超额沪深300比率'].cumprod()
        self.__benchStandard['策略对冲中证500'] = exceed_return_ratio['策略超额中证500比率'].cumprod()
        self.__benchStandard['策略对冲中证1000'] = exceed_return_ratio['策略超额中证1000比率'].cumprod()
        self.__benchStandard['策略对冲创业板指'] = exceed_return_ratio['策略超额创业板指比率'].cumprod()
        self.__benchStandard['策略对冲中证800'] = exceed_return_ratio['策略超额中证800比率'].cumprod()
        self.__benchStandard['策略对冲中证2000'] = exceed_return_ratio['策略超额中证2000比率'].cumprod()

        # 计算与自定义指数对冲后净值
        if not self.__user_benchmark.empty:
            for (index, hedgeIndex) in zip(self.__user_bench_name_list, self.__user_bench_hedge_list):
                exceed_return_ratio[hedgeIndex] = bench_change[strategy_name] - bench_change[index] + 1
                self.__benchStandard[hedgeIndex] = exceed_return_ratio[hedgeIndex].cumprod()

    def process(self, strategyName, riskFreeRate):
        self.calculateReturn(strategyName)
        self.calculateMaxdrawdown(strategyName)
        self.calculateSharpeRatio(strategyName, riskFreeRate)

    def getBenchCombine(self):
        return self.__benchCombine
    
    def getBenchStandard(self):
        return self.__benchStandard

    def benchmark_with_base(self):
        if self.__config.price_type != PriceType.CLOSE:
            return bench_util.add_net_value_base(self.__benchStandard)
        else:
            return self.__benchStandard.copy()



