# -*- coding: utf-8 -*-
"""
Created on Tue Jul 04 13:35:07 2017

@author: cxd

comment: 
回测报告输出模块，将回测结果的输出进行封装处理
"""


import math
import copy
import pandas as pd
import pathlib

from wk_platform.stratanalyzer import returns
from wk_platform.stratanalyzer import sharpe
from wk_platform.stratanalyzer import drawdown
from wk_platform.stratanalyzer import tracker
from wk_platform.backtest import benchmark
from wk_platform.backtest.result import BackTestResult
from wk_platform.backtest.analyzer import *
from wk_platform.strategy.strategy import BaseStrategy
from wk_platform.broker.commission import TradePercentage
from wk_platform.broker.commission import TradePercentageTaxFee
from wk_platform.config import StrategyConfiguration
from wk_analyzer.plot.backtest import plot_net_value, plot_drawback


def reformat_row(data):
    if isinstance(data, pd.DataFrame):

        rows = data.index
        fixed_order = [
            "myStrategy",
            "沪深300",
            "中证500",
            "中证800",
            "中证1000",
            "中证2000",
            "策略对冲沪深300",
            "策略对冲中证500",
            "策略对冲中证800",
            "策略对冲中证1000",
            "策略对冲中证2000",
            "上证综指",
            "上证50",
            "创业板指",
            "策略对冲上证综指",
            "策略对冲上证50",
            "策略对冲创业板指",

        ]
        remain_set = set(rows) - set(fixed_order)
        remain_order = []
        for k in rows:
            if k in remain_set:
                remain_order.append(k)
        data = data.loc[ fixed_order + remain_order]
        return data
    else:
        assert isinstance(data, OrderedDict)
        new_data = OrderedDict()
        fields = [
            "myStrategy",
            "策略对冲沪深300",
            "策略对冲中证500",
            "策略对冲中证800",
            "策略对冲中证1000",
            "策略对冲中证2000",
            "策略对冲上证综指",
            "策略对冲上证50",
            "策略对冲创业板指"
        ]
        for k in fields:
            new_data[k] = data[k]
        remain_fields = set(data.keys()) - set(fields)
        for k in data.keys():
            if k in remain_fields:
                new_data[k] = data[k]
        return new_data


class StrategyOutput:
    """
    StrategyOutput：回测报告输出类，计算各类回测指标
    增加参数strategyName,默认参数为myStrategy
    """
    def __init__(self, strategy: BaseStrategy, begin_date, end_date, strategy_name='myStrategy',
                 config=StrategyConfiguration(), user_benchmark=None):
        """
        注册策略和相关参数
        """
        if user_benchmark is None:
            user_benchmark = pd.DataFrame()

        self.__config = config

        self.__result = None
        self.__strategy = strategy
        
        self.__begin_date = begin_date
        self.__end_date = end_date

        # 获取策略的重要的回测评价指标
        self.__retAnalyzer = returns.Returns()

        # 注意sharpeRatio的参数
        self.__sharpeRatioAnalyzer = sharpe.SharpeRatio()
        self.__drawDownAnalyzer = drawdown.DrawDown()

        self.__strategy_tracker = tracker.StrategyTracker()
        self.__strategy_custom_tracker = tracker.StrategyCustomTracker()
        
        #self.__riskFreeRate = 0.05
        self.__risk_free_rate = self.__config.risk_free_rate
        
        """
        每年交易日
        """
        self.__tradeDaysOneYear = 365
        
        """
        记录策略名字
        """
        self.__strategyName = strategy_name
        
        """
        记录用户自定义的基准, 20171228
        """
        self.__user_benchmark = user_benchmark
        
        """
        添加对benchmark的分析
        """
        self.__bench = benchmark.BenchmarkAnalyzer(self.__begin_date, self.__end_date, self.__user_benchmark,
                                                   config=strategy.config)

    def pre_process(self):
        """
        预处理中完成对回测指标的注册
        """
        # print("stra_pre_process")
        # self.__strategy.attachAnalyzer(self.__retAnalyzer)
        # self.__strategy.attachAnalyzer(self.__sharpeRatioAnalyzer)
        # self.__strategy.attachAnalyzer(self.__drawDownAnalyzer)
        self.__strategy.attachAnalyzer(self.__strategy_tracker)

        self.__strategy.attachAnalyzerEx(self.__strategy_custom_tracker, 'custom_analyzer')


    def bench_process(self):
        """
        benchmark指标的分析处理
        """

        """
        获取策略总equity，用来计算净值
        """
        # strategy_equity = self.__retAnalyzer.getPdSharesTemp()
        strategy_equity = self.__strategy_tracker.total_position_records
        """
        读取基准指数的数据文件
        新增参数，策略名称
        """
        self.__bench.prepare_benchmark(strategy_equity, self.__strategyName)
        
        """
        计算基准指数的各项指标
        新增参数，策略名称
        """
        self.__bench.process(self.__strategyName, self.__risk_free_rate)



    def post_process(self):
        """
        策略运行完成后，完成对回测报告的输出
        """

        """
        记录策略的交易流水
        """
        # tracker = self.__retAnalyzer.getPdTradeTrackerTemp()

        """
        记录策略的每日持仓
        """
        # share1 = self.__retAnalyzer.getPdSharesTemp()
        share = self.__strategy_tracker.total_position_records
        """
        记录策略的每日详细持仓
        """
        # shareDetails = self.__retAnalyzer.getPdDetailsTemp()
        shareDetails = self.__strategy_tracker.position_records
        """
        记录个股详细持仓变化
        """
        shareDetailPositions = self.__strategy_tracker.detailed_position_records

        unfilled_orders = self.__strategy_tracker.unfilled_orders
        transaction_records = self.__strategy_tracker.transaction_records

        context = AnalyzerContext()
        analyzers = [
            BasicInfoAnalyzer(self.__strategy, self.__strategyName, self.__config),
            ConfigSummaryAnalyzer(),
            BenchAnalyzer(self.__bench),
            DailyMetricAnalyzer(),
            WeeklyMetricAnalyzer(),
            MonthlyMetricAnalyzer(),
            YearlyMetricAnalyzer(),
            BetaAnalyzer(),
            AlphaAnalyzer(),
            InfoRatioAnalyzer(),
            DrawDownAnalyzer(self.__bench),
            DailyWinRatioAnalyzer(),
            WeeklyWinRatioAnalyzer(),
            MonthlyWinRatioAnalyzer(),
            MonthlyDrawDownAnalyzer(),
            MonthlyRelativeWinRatioAnalyzer(),
            MonthlyRelativeWinRatioUpAnalyzer(),
            MonthlyRelativeWinRatioDownAnalyzer(),
            WinLoseAmountRatioAnalyzer(),
            MetricSummaryAnalyzer(self.__bench),
            YearlyDrawDownAnalyzer(),
            YearlyExtendedMetricAnalyzer(),
            MonthlyAggregatedMetricAnalyzer()
        ]

        for analyzer in analyzers:
            analyzer(context)

        result = BackTestResult()

        result.add_metric('回测配置', context[ConfigSummaryAnalyzer].config_summary)
        result.add_metric('策略指标', context[MetricSummaryAnalyzer].metric_summary, formatter=reformat_row)
        result.add_metric('年度表现', context[YearlyExtendedMetricAnalyzer].summary, formatter=reformat_row)
        result.add_metric('策略净值', context[BenchAnalyzer].benchmark, plot_func=plot_net_value)
        result.add_metric('回撤指标', context[DrawDownAnalyzer].draw_down_summary, formatter=reformat_row)
        result.add_metric('回撤详情', context[DrawDownAnalyzer].draw_down, plot_func=plot_drawback)
        result.add_metric('分月度表现', context[MonthlyAggregatedMetricAnalyzer].summary, formatter=reformat_row)

        if self.__strategy.custom_analyzer is not None:
            for k, df in self.__strategy.custom_analyzer.entries():
                result.add_metric(k, df)

        result.add_metric('月频涨跌幅', context[MonthlyMetricAnalyzer].monthly_change)
        result.add_metric('周频涨跌幅', context[WeeklyMetricAnalyzer].weekly_change)
        # result.add_metric('交易流水', tracker.set_index("交易日期"))
        result.add_metric('交易流水', transaction_records.set_index("交易日期"))
        result.add_metric('未成交记录', unfilled_orders.set_index("交易时间"))
        result.add_metric('每日持仓', share.set_index("日期"))
        result.add_metric('详细持仓', shareDetails)
        result.add_metric('个股跟踪', shareDetailPositions.set_index("交易日期"))

        # result.add_metric('日胜率', context[DailyWinRatioAnalyzer])

        # 增加组合年收益率和月收益率
        result.add_metric('月频净值截面', context[MonthlyMetricAnalyzer].benchmark_monthly)
        result.add_metric('年频净值截面', context[YearlyMetricAnalyzer].benchmark_yearly)

        result.add_metric('年频涨跌幅', context[YearlyMetricAnalyzer].yearly_change),
        # result.add_metric('年度夏普比', context[YearlyExtendedMetricAnalyzer].yearly_sharpe)
        # result.add_metric('年度卡玛比', context[YearlyExtendedMetricAnalyzer].yearly_calmar)
        # result.add_metric('年度回撤', context[YearlyExtendedMetricAnalyzer].yearly_draw_down)
        # result.add_metric('年度月胜率', context[YearlyExtendedMetricAnalyzer].yearly_month_win_ratio)



        self.__result = result

    @property
    def result(self):
        if self.__result is None:
            raise ValueError("call `post_process` before using result")
        return self.__result
        
        
# 此部分代码用于指标计算参考
    # def post_process_ref(self):
    #     """
    #     策略运行完成后，完成对回测报告的输出
    #     原始实现，保留用于参考
    #     """
    #     """
    #     strategyName = 'myStrategy'
    #     """
    #     print("stra_post_process")
    #
    #     """
    #     获得策略的评价指标
    #     """
    #     maxdrawdown = self.__drawDownAnalyzer.getMaxDrawDown()
    #     longestDrawDownTime = self.__drawDownAnalyzer.getLongestDrawDownDuration()
    #     highTime = self.__drawDownAnalyzer.getHighDateTimes()
    #     lowTime = self.__drawDownAnalyzer.getLowDateTimes()
    #     highTime = highTime.strftime("%Y-%m-%d")
    #     lowTime = lowTime.strftime("%Y-%m-%d")
    #
    #     return_ = self.__retAnalyzer.getCumulativeReturns()[-1]
    #     annual_return = self.__retAnalyzer.getAnnualReturns()
    #     volatility = self.__sharpeRatioAnalyzer.getVolatility()
    #     sharp = self.__sharpeRatioAnalyzer.getSharpeRatio(self.__riskFreeRate)
    #
    #
    #
    #     # """
    #     # 记录策略的交易流水
    #     # """
    #     # tracker = self.__retAnalyzer.getPdTradeTrackerTemp()
    #     # """
    #     # 记录策略的每日持仓
    #     # """
    #     # # share = self.__retAnalyzer.getPdSharesTemp()
    #     # share = self.__strategy_tracker.total_position_records
    #     # """
    #     # 记录策略的每日详细持仓
    #     # """
    #     # shareDetails = self.__retAnalyzer.getPdDetailsTemp()
    #     # """
    #     # 记录个股详细持仓变化
    #     # """
    #     # shareDetailPositions = self.__retAnalyzer.detailed_position_records
    #     #
    #     # unfilled_orders = self.__retAnalyzer.unfilled_orders
    #     # transaction_records = self.__retAnalyzer.transaction_records
    #
    #     """
    #     记录策略的交易流水
    #     """
    #     # tracker = self.__retAnalyzer.getPdTradeTrackerTemp()
    #     """
    #     记录策略的每日持仓
    #     """
    #     # share = self.__retAnalyzer.getPdSharesTemp()
    #     share = self.__strategy_tracker.total_position_records
    #     """
    #     记录策略的每日详细持仓
    #     """
    #     shareDetails = self.__strategy_tracker.position_records
    #     """
    #     记录个股详细持仓变化
    #     """
    #     shareDetailPositions = self.__strategy_tracker.detailed_position_records
    #
    #     unfilled_orders = self.__strategy_tracker.unfilled_orders
    #     transaction_records = self.__strategy_tracker.transaction_records
    #
    #     """
    #     获取净值列表
    #     """
    #     benchIndex = self.__bench.getBenchStandard()
    #     benchIndex = benchIndex.reset_index()
    #
    #     """
    #     对于净值列表进行月度采样,月末采样
    #     """
    #     benchIndexMonthly = copy.deepcopy(benchIndex)
    #     #benchIndexMonthly =benchIndexMonthly.reset_index()
    #     benchIndexMonthly['date'] = pd.to_datetime(benchIndexMonthly['date'], format = '%Y%m%d')
    #     benchIndexMonthly.set_index('date', inplace=True)
    #     benchIndexMonthly = benchIndexMonthly.resample('M').ffill()
    #
    #
    #     """
    #     对于净值列表进行年度采样,年末采样
    #     """
    #     benchIndexYearly = copy.deepcopy(benchIndex)
    #     #benchIndexYearly =benchIndexYearly.reset_index()
    #     benchIndexYearly['date'] = pd.to_datetime(benchIndexYearly['date'], format = '%Y%m%d')
    #     benchIndexYearly.set_index('date', inplace = True)
    #     benchIndexYearly = benchIndexYearly.resample('BA-DEC').ffill()
    #
    #
    #
    #     benchIndexMonthly = benchIndexMonthly.reset_index()
    #     benchIndexMonthly['date'] = [x.strftime('%Y%m%d') for x in benchIndexMonthly['date']]
    #     """
    #     新增日频数据第一行到月频截面，用日频数据最后一行替换月频数据最后一行时间
    #     解决进行月频采样时的边界问题
    #     """
    #     benchIndexMonthly = benchIndexMonthly.append(benchIndex.iloc[0:1])
    #     benchIndexMonthly = benchIndexMonthly.sort_values(by='date')
    #     benchIndexMonthly.drop(benchIndexMonthly.index[-1], inplace = True)
    #     benchIndexMonthly = benchIndexMonthly.append(benchIndex.iloc[-1:])
    #     benchIndexMonthly = benchIndexMonthly.set_index('date')
    #
    #
    #     benchIndexYearly = benchIndexYearly.reset_index()
    #     benchIndexYearly['date'] = [x.strftime('%Y%m%d') for x in benchIndexYearly['date']]
    #     benchIndexYearly = benchIndexYearly.append(benchIndex.iloc[0:1])
    #     benchIndexYearly = benchIndexYearly.sort_values(by='date')
    #     benchIndexYearly.drop(benchIndexYearly.index[-1], inplace=True)
    #     benchIndexYearly = benchIndexYearly.append(benchIndex.iloc[-1:])
    #     benchIndexYearly = benchIndexYearly.set_index('date')
    #
    #
    #
    #     """
    #     日期列设为索引，使得可以调用pct_change
    #     """
    #     benchIndex = benchIndex.set_index('date')
    #
    #
    #     """
    #     得出日收益率矩阵
    #     """
    #     dailyChange = benchIndex.pct_change()
    #     dailyDiff = benchIndex.diff()
    #     """
    #     删除首行Null值
    #     """
    #     dailyChange = dailyChange.dropna()
    #     dailyDiff = dailyDiff.dropna()
    #
    #
    #     """
    #     得出月收益率矩阵
    #     """
    #     monthlyChange = benchIndexMonthly.pct_change()
    #     monthlyDiff = benchIndexMonthly.diff()
    #     monthlyChange = monthlyChange.dropna()
    #     monthlyDiff = monthlyDiff.dropna()
    #
    #     """
    #     得出年收益率矩阵
    #     """
    #     yearlyChange = benchIndexYearly.pct_change()
    #     yearlyChange = yearlyChange.dropna()
    #
    #     """
    #     计算beta值，策略相对于基准
    #     """
    #     betaDict = {}
    #     # for symbol in self.__bench.getNameList():
    #     for symbol in self.__bench.getAllBenchList():
    #         if dailyChange[symbol].var() !=0:
    #             betaDict[symbol] = dailyChange[symbol].cov(dailyChange[self.__strategyName])/dailyChange[symbol].var()
    #         else:
    #             betaDict[symbol] = '--'
    #
    #     betaDict[self.__strategyName] = '--'
    #     # for symbol in self.__bench.getHedgeList():
    #     for symbol in self.__bench.getAllHedgeList():
    #         betaDict[symbol] = '--'
    #
    #     """
    #     计算alpha值
    #     alpha = Rp - (Rf + beta *(Rm - Rf))
    #
    #     """
    #     alphaDict = {}
    #     # for symbol in self.__bench.getNameList():
    #     for symbol in self.__bench.getAllBenchList():
    #         if betaDict[symbol] != '--':
    #             # alphaDict[symbol] = annual_return - (self.__riskFreeRate + betaDict[symbol]*(self.__bench.getAnnualReturn()[symbol] - self.__riskFreeRate))
    #             alphaDict[symbol] = self.__bench.getAnnualReturn()[self.__strategyName] - (self.__riskFreeRate + betaDict[symbol]*(self.__bench.getAnnualReturn()[symbol] - self.__riskFreeRate))
    #         else:
    #             alphaDict[symbol] = '--'
    #     # print alphaDict
    #     alphaDict[self.__strategyName] = '--'
    #     # for symbol in self.__bench.getHedgeList():
    #     for symbol in self.__bench.getAllHedgeList():
    #         alphaDict[symbol] = '--'
    #
    #     """
    #     计算信息比率  (策略年化收益率 - 基准年化收益率) / (策略与基准每日收益差值的年化标准差)
    #     """
    #     infoRatio = {}
    #     # for symbol in self.__bench.getNameList():
    #     for symbol in self.__bench.getAllBenchList():
    #         # returnDiff = annual_return - self.__bench.getAnnualReturn()[symbol]
    #         returnDiff = self.__bench.getAnnualReturn()[self.__strategyName] - self.__bench.getAnnualReturn()[symbol]
    #         returnDiffList = dailyChange[self.__strategyName] - dailyChange[symbol]
    #         diffStd = (returnDiffList.std()) * (math.sqrt(250))
    #
    #         if diffStd !=0:
    #             infoRatio[symbol] = returnDiff/diffStd
    #         else:
    #             infoRatio[symbol] = '--'
    #     # print infoRatio
    #     infoRatio[self.__strategyName] = '--'
    #     # for symbol in self.__bench.getHedgeList():
    #     for symbol in self.__bench.getAllHedgeList():
    #         infoRatio[symbol] = '--'
    #
    #
    #
    #
    #     """
    #     日胜率统计
    #     """
    #     """
    #     dailyWinRatioDict = {}
    #
    #     strategyDailyWinCount = dailyChange[dailyChange[strategyName] >= 0][strategyName].count()
    #     strategyDailyLoseCount = dailyChange[dailyChange[strategyName] < 0][strategyName].count()
    #
    #     print strategyDailyWinCount
    #     print strategyDailyLoseCount
    #
    #     if strategyDailyLoseCount !=0:
    #         dailyWinRatioDict[strategyName] = strategyDailyWinCount/float(strategyDailyLoseCount)
    #     else:
    #         dailyWinRatioDict[strategyName] = u'全胜'
    #
    #     for symbol in self.__bench.getNameList():
    #         print symbol
    #         relativeDailyWinCount =  dailyChange[dailyChange[strategyName] >= dailyChange[symbol]][strategyName].count()
    #         relativeDailyLoseCount = dailyChange[dailyChange[strategyName] < dailyChange[symbol]][strategyName].count()
    #
    #         print relativeDailyWinCount
    #         print relativeDailyLoseCount
    #
    #         if relativeDailyLoseCount !=0:
    #             dailyWinRatioDict[symbol] = relativeDailyWinCount/float(relativeDailyLoseCount)
    #         else:
    #             dailyWinRatioDict[symbol] = u'全胜'
    #     for symbol in self.__bench.getHedgeList():
    #         dailyWinRatioDict[symbol] = u'--'
    #
    #     print 'daily win ratio'
    #     print dailyWinRatioDict
    #     """
    #
    #
    #     """
    #     月胜率统计，绝对胜率
    #     """
    #     """
    #     print monthlyChange
    #     """
    #     monthlyWinRatioDict = {}
    #     # totalList = [self.__strategyName] + self.__bench.getNameList() + self.__bench.getHedgeList()
    #     totalList = [self.__strategyName] + self.__bench.getAllBenchList() + self.__bench.getAllHedgeList()
    #     for symbol in totalList:
    #         strategyMonthlyWinCount = monthlyChange[monthlyChange[symbol] >= 0][symbol].count()
    #         strategyMonthlyLoseCount = monthlyChange[monthlyChange[symbol] < 0][symbol].count()
    #
    #         """
    #         print symbol
    #         print strategyMonthlyWinCount
    #         print strategyMonthlyLoseCount
    #         """
    #         if strategyMonthlyLoseCount!=0:
    #             monthlyWinRatioDict[symbol] = strategyMonthlyWinCount/float(strategyMonthlyLoseCount+strategyMonthlyWinCount)
    #         else:
    #             monthlyWinRatioDict[symbol] = '100%'
    #
    #
    #     """
    #     月胜率统计，策略相对基准月胜率
    #     """
    #     monthlyRelativeWinRatioDict = {}
    #
    #     # for symbol in self.__bench.getNameList():
    #     for symbol in self.__bench.getAllBenchList():
    #         # print symbol
    #         relativeMonthlyWinCount =  monthlyChange[monthlyChange[self.__strategyName] >= monthlyChange[symbol]][self.__strategyName].count()
    #         relativeMonthlyLoseCount = monthlyChange[monthlyChange[self.__strategyName] < monthlyChange[symbol]][self.__strategyName].count()
    #         """
    #         print symbol
    #         print monthlyChange[monthlyChange[strategyName] >= monthlyChange[symbol]]
    #         print monthlyChange[monthlyChange[strategyName] < monthlyChange[symbol]]
    #         print relativeMonthlyWinCount
    #         print relativeMonthlyLoseCount
    #         """
    #         if relativeMonthlyLoseCount!= 0:
    #             monthlyRelativeWinRatioDict[symbol] = relativeMonthlyWinCount/float(relativeMonthlyLoseCount+relativeMonthlyWinCount)
    #         else:
    #             monthlyRelativeWinRatioDict[symbol] = '100%'
    #     # for symbol in self.__bench.getHedgeList():
    #     for symbol in self.__bench.getAllHedgeList():
    #         monthlyRelativeWinRatioDict[symbol] = '--'
    #     monthlyRelativeWinRatioDict[self.__strategyName] = '--'
    #     """
    #     print 'monthly win ratio'
    #     print monthlyRelativeWinRatioDict
    #     """
    #
    #     """
    #     相对月胜率统计,基准上涨时相对月胜率
    #     """
    #     monthlyRelativeWinRatioUpDict = {}
    #     # for symbol in self.__bench.getNameList():
    #     for symbol in self.__bench.getAllBenchList():
    #         # print symbol
    #         monthlyChangeUp = monthlyChange[monthlyChange[symbol] >= 0]
    #         relativeMonthlyWinCount =  monthlyChangeUp[monthlyChangeUp[self.__strategyName] >= monthlyChangeUp[symbol]][self.__strategyName].count()
    #         relativeMonthlyLoseCount = monthlyChangeUp[monthlyChangeUp[self.__strategyName] < monthlyChangeUp[symbol]][self.__strategyName].count()
    #         """
    #         print symbol
    #         print monthlyChange[monthlyChange[strategyName] >= monthlyChange[symbol]]
    #         print monthlyChange[monthlyChange[strategyName] < monthlyChange[symbol]]
    #         print relativeMonthlyWinCount
    #         print relativeMonthlyLoseCount
    #         """
    #         if relativeMonthlyLoseCount!= 0:
    #             monthlyRelativeWinRatioUpDict[symbol] = relativeMonthlyWinCount/float(relativeMonthlyLoseCount+relativeMonthlyWinCount)
    #         else:
    #             monthlyRelativeWinRatioUpDict[symbol] = '100%'
    #     #f or symbol in self.__bench.getHedgeList():
    #     for symbol in self.__bench.getAllHedgeList():
    #         monthlyRelativeWinRatioUpDict[symbol] = '--'
    #     monthlyRelativeWinRatioUpDict[self.__strategyName] = '--'
    #
    #
    #     """
    #     相对月胜率统计,基准下跌时相对月胜率
    #     """
    #     monthlyRelativeWinRatioDownDict = {}
    #     #for symbol in self.__bench.getNameList():
    #     for symbol in self.__bench.getAllBenchList():
    #         #print symbol
    #         monthlyChangeDown = monthlyChange[monthlyChange[symbol] < 0]
    #         relativeMonthlyWinCount =  monthlyChangeDown[monthlyChangeDown[self.__strategyName] >= monthlyChangeDown[symbol]][self.__strategyName].count()
    #         relativeMonthlyLoseCount = monthlyChangeDown[monthlyChangeDown[self.__strategyName] < monthlyChangeDown[symbol]][self.__strategyName].count()
    #         """
    #         print symbol
    #         print monthlyChange[monthlyChange[strategyName] >= monthlyChange[symbol]]
    #         print monthlyChange[monthlyChange[strategyName] < monthlyChange[symbol]]
    #         print relativeMonthlyWinCount
    #         print relativeMonthlyLoseCount
    #         """
    #         if relativeMonthlyLoseCount!= 0:
    #             monthlyRelativeWinRatioDownDict[symbol] = relativeMonthlyWinCount/float(relativeMonthlyLoseCount+relativeMonthlyWinCount)
    #         else:
    #             monthlyRelativeWinRatioDownDict[symbol] = '100%'
    #     #for symbol in self.__bench.getHedgeList():
    #     for symbol in self.__bench.getAllHedgeList():
    #         monthlyRelativeWinRatioDownDict[symbol] = '--'
    #     monthlyRelativeWinRatioDownDict[self.__strategyName] = '--'
    #
    #
    #
    #     """
    #     盈亏比计算
    #     """
    #     winLoseAmountRatioDict = {}
    #     #totalList = [self.__strategyName] + self.__bench.getNameList() + self.__bench.getHedgeList()
    #     totalList = [self.__strategyName] + self.__bench.getAllBenchList() + self.__bench.getAllHedgeList()
    #     for symbol in totalList:
    #         winAmount = dailyDiff[dailyDiff[symbol] >=0][symbol].sum()
    #         loseAmount = dailyDiff[dailyDiff[symbol] <0][symbol].sum()
    #         """
    #         print winAmount
    #         print loseAmount
    #         """
    #         if loseAmount !=0:
    #             winLoseAmountRatioDict[symbol] = abs(winAmount/float(loseAmount))
    #         else:
    #             winLoseAmountRatioDict[symbol] = '100%'
    #     """
    #     print winLoseAmountRatioDict
    #     """
    #
    #     """
    #     记录策略评价指标,策略与基准指数使用同样的计算函数, 20171025
    #     """
    #     """
    #     拼接基准指数的评价指标
    #     """
    #     """
    #     totalList = self.__bench.getNameList()+self.__bench.getHedgeList()
    #     strategyIndex = pd.DataFrame([[return_,annual_return,volatility,maxdrawdown,sharp, infoRatio[self.__strategyName], betaDict[self.__strategyName],alphaDict[self.__strategyName], monthlyWinRatioDict[self.__strategyName],monthlyRelativeWinRatioDict[self.__strategyName],winLoseAmountRatioDict[self.__strategyName]]], columns = [u'策略收益',u'年化收益率',u'波动率', u'最大回撤',u'夏普比率',u'信息比率',u'贝塔',u'阿尔法',u'月胜率',u'相对基准月胜率',u'盈亏比'], index=[self.__strategyName])
    #     """
    #
    #     totalList = [self.__strategyName] + self.__bench.getAllBenchList() + self.__bench.getAllHedgeList()
    #     strategy_metric = pd.DataFrame()
    #     for symbol in totalList:
    #         df_temp = pd.DataFrame([[
    #             self.__bench.getCumulativeReturn()[symbol],
    #             self.__bench.getAnnualReturn()[symbol],
    #             self.__bench.getVolatility()[symbol],
    #             self.__bench.getMaxDrawdown()[symbol],
    #             self.__bench.getSharpeRatio()[symbol],
    #             infoRatio[symbol],
    #             betaDict[symbol],
    #             alphaDict[symbol],
    #             monthlyWinRatioDict[symbol],
    #             monthlyRelativeWinRatioDict[symbol],
    #             monthlyRelativeWinRatioUpDict[symbol],
    #             monthlyRelativeWinRatioDownDict[symbol],
    #             winLoseAmountRatioDict[symbol]
    #         ]], columns=[
    #             '策略收益', '年化收益率', '波动率', '最大回撤', '夏普比率', '信息比率',
    #             '贝塔', '阿尔法', '月胜率', '相对基准月胜率', '基准上涨相对月胜率', '基准下跌相对月胜率', '盈亏比'
    #         ], index=[symbol])
    #         strategy_metric = strategy_metric.append(df_temp)
    #
    #
    #     """
    #     获取最大回撤的评价指标
    #     """
    #     #drawDownIndex = pd.DataFrame([[maxdrawdown,longestDrawDownTime,highTime,lowTime]], columns = [u'最大回撤', u'回撤时长',u'高点时间',u'低点时间'],index = [self.__strategyName])
    #     drawDownIndex = pd.DataFrame()
    #     for symbol in totalList:
    #         dfTemp = pd.DataFrame([[
    #             self.__bench.getMaxDrawdown()[symbol],
    #             self.__bench.getMaxDrawdowTime()[symbol],
    #             self.__bench.getMaxDrawdownHigh()[symbol],
    #             self.__bench.getMaxDrawdownLow()[symbol]
    #         ]], columns=[
    #                 '最大回撤', '回撤时长', '高点时间', '低点时间'
    #         ], index=[symbol])
    #         drawDownIndex = drawDownIndex.append(dfTemp)
    #
    #     """
    #     不显示对冲净值
    #     """
    #     """
    #     benchList = [u'日期']+self.__bench.getNameList()+ [strategyName]
    #     benchIndex = benchIndex.reset_index()
    #     benchIndex = benchIndex[benchList]
    #     benchIndex = benchIndex.set_index(u'日期')
    #     """
    #     config = self.__strategy.config
    #
    #     config_summary = {
    #         "初始资金": config.initial_cash,
    #         "交易价格": config.price_type,
    #         "涨跌停限制交易": config.max_up_down_limit,
    #         "停复牌限制交易": config.suspension_limit,
    #         "交易量限制": config.volume_limit,
    #         "佣金费率": config.commission.percentage if isinstance(config.commission, TradePercentage) else 0,
    #         "印花税率": config.stamp_tax.percentage if isinstance(config.stamp_tax, TradePercentageTaxFee ) else 0
    #     }
    #     entry_names = []
    #     entry_values = []
    #     for k, v in config_summary.items():
    #         entry_names.append(k)
    #         entry_values.append(str(v))
    #
    #     config_summary_df = pd.DataFrame({
    #         "项目": entry_names,
    #         "说明": entry_values
    #     }).set_index('项目')
    #
    #
    #     result = BackTestResult()
    #
    #     result.add_metric('回测配置', config_summary_df)
    #     result.add_metric('策略指标', strategy_metric)
    #     result.add_metric('回撤详情', drawDownIndex)
    #     # result.add_metric('交易流水', tracker.set_index("交易日期"))
    #     result.add_metric('交易流水', transaction_records.set_index("交易日期"))
    #     result.add_metric('未成交记录', unfilled_orders.set_index("交易时间"))
    #     result.add_metric('每日持仓', share.set_index("日期"))
    #     result.add_metric('详细持仓', shareDetails)
    #     result.add_metric('个股跟踪', shareDetailPositions.set_index("交易日期"))
    #     result.add_metric('策略净值', benchIndex)
    #
    #     # 增加组合年收益率和月收益率
    #     result.add_metric('月频净值截面', benchIndexMonthly)
    #     result.add_metric('年频净值截面', benchIndexYearly)
    #     result.add_metric('月频涨跌幅', monthlyChange)
    #     result.add_metric('年频涨跌幅', yearlyChange)
    #
    #     self.__result = result
        
        
        
        
        
        
    

