"""

各类回测指标的计算算子

"""
import pandas as pd
import copy
import math
from collections import OrderedDict

import numpy as np

from wk_platform.broker.commission import TradePercentage, StampTaxAShare
from wk_platform.broker.commission import TradePercentageTaxFee
from wk_platform.config import StrategyConfiguration, HedgeStrategyConfiguration
from wk_platform.stratanalyzer.sharpe import sharpe_ratio_3
from wk_platform.stratanalyzer.drawdown import draw_down as calc_draw_down
import wk_platform.backtest.util as bench_util


class AnalyzerContext:
    """
    保存指标的计算图
    """

    def __init__(self):
        self.__analyzers_status: dict[str: bool] = {}
        self.__analyzers: dict[str: BaseAnalyzer] = {}

    def add_analyzer(self, analyzer):
        key = analyzer.__class__.__name__
        try:
            _ = self.__analyzers[key]
            raise ValueError("Duplicated analyzer")
        except KeyError:
            self.__analyzers[key] = analyzer
            self.__analyzers_status[key] = True

    def check_finished(self, analyzer_class):
        return self.__analyzers_status[analyzer_class.__name__]

    def __getitem__(self, analyzer_class):
        return self.__analyzers[analyzer_class.__name__]


class BaseAnalyzer:
    def __init__(self, dependencies=None):
        self.__dependencies = []
        if dependencies is not None:
            self.__dependencies += dependencies

    def __check_dependencies(self, context: AnalyzerContext) -> bool:
        for dep in self.__dependencies:
            if not context.check_finished(dep):
                return False
        return True

    def _analyze(self, context: AnalyzerContext):
        raise NotImplementedError()

    def __call__(self, context: AnalyzerContext):
        if not self.__check_dependencies(context):
            raise ValueError("calculation dependencies check failed")
        self._analyze(context)
        context.add_analyzer(self)


class Depend:
    """
    描述算子间依赖关系的装饰器
    """
    def __init__(self, *class_args):
        self.__class_args = class_args

    def __call__(self, cls):
        def func(*args, **kwargs):
            obj = cls(*args, **kwargs)
            super(type(obj), obj).__init__(self.__class_args)
            return obj

        func.__name__ = cls.__name__
        return func


class BasicInfoAnalyzer(BaseAnalyzer):
    def __init__(self, strategy, strategy_name,  config: StrategyConfiguration):
        super().__init__()
        self.__strategy = strategy
        self.__strategy_name = strategy_name
        self.__config = config
        self.__risk_free_rate = config.risk_free_rate

    @property
    def risk_free_rate(self):
        return self.__risk_free_rate

    @property
    def config(self):
        return self.__config

    def _analyze(self, context: AnalyzerContext):
        setattr(self, "strategy_name", self.__strategy_name)
        setattr(self, "begin_date", self.__strategy.begin_date)
        setattr(self, "end_date", self.__strategy.end_date)
        setattr(self, "sign", self.__strategy.sign)


class BenchAnalyzer(BaseAnalyzer):
    def __init__(self, bench_ins):
        super().__init__()
        self.__bench_ins = bench_ins

    @property
    def benchmark(self):
        return self.__bench_ins.getBenchStandard().copy()

    @property
    def benchmark_with_base(self):
        return self.__bench_ins.benchmark_with_base()

    # def remove_base(self, data):
    #     return self.__bench_ins.remove_net_value_base(data)

    @property
    def annual_return(self):
        return self.__bench_ins.getAnnualReturn()

    @property
    def hedge_list(self):
        return self.__bench_ins.getHedgeList()

    @property
    def name_list(self):
        return self.__bench_ins.getNameList()

    @property
    def full_list(self):
        return self.__bench_ins.getFullList()

    def _analyze(self, context: AnalyzerContext):
        setattr(self, "all_bench_list", self.__bench_ins.getAllBenchList())
        setattr(self, "all_hedge_list", self.__bench_ins.getAllHedgeList())
        # setattr(self, "benchmark", self.__bench_ins.getBenchStandard().reset_index())


@Depend(BasicInfoAnalyzer)
class ConfigSummaryAnalyzer(BaseAnalyzer):
    def _analyze(self, context: AnalyzerContext):

        config = context[BasicInfoAnalyzer].config
        begin_date = context[BasicInfoAnalyzer].begin_date
        end_date = context[BasicInfoAnalyzer].end_date
        sign = context[BasicInfoAnalyzer].sign
        if  isinstance(config.stamp_tax, TradePercentageTaxFee):
            stamp_tax_desc = config.stamp_tax.percentage
        elif isinstance(config.stamp_tax, StampTaxAShare):
            stamp_tax_desc = config.stamp_tax.desc
        else:
            stamp_tax_desc = 0

        config_summary = {
            "平台版本": config.version,
            "回测区间": f"{begin_date}-{end_date}",
            "策略类型": config.type,
            "初始资金": config.initial_cash,
            "交易价格": config.price_type,
            "涨跌停限制交易": config.max_up_down_limit,
            "停复牌限制交易": config.suspension_limit,
            "交易量限制": config.volume_limit,
            "无风险利率": config.risk_free_rate,
            "佣金费率": config.commission.percentage if isinstance(config.commission, TradePercentage) else 0,
            "印花税率": stamp_tax_desc,
            "权重文件md5摘要": sign
        }
        if isinstance(config, HedgeStrategyConfiguration):
            config_summary['对冲标的'] = config.code
        entry_names = []
        entry_values = []
        for k, v in config_summary.items():
            entry_names.append(k)
            entry_values.append(str(v))

        config_summary_df = pd.DataFrame({
            "项目": entry_names,
            "说明": entry_values
        }).set_index('项目')

        setattr(self, "config_summary", config_summary_df)


@Depend(BenchAnalyzer)
class DailyMetricAnalyzer(BaseAnalyzer):
    def _analyze(self, context: AnalyzerContext):
        """
        得出日收益率矩阵
        """
        # benchmark = context[BenchAnalyzer].benchmark.set_index('date')
        benchmark = context[BenchAnalyzer].benchmark_with_base
        daily_change = benchmark.pct_change()
        daily_diff = benchmark.diff()

        # 删除首行Null值
        daily_change = daily_change.dropna()
        daily_diff = daily_diff.dropna()

        setattr(self, "daily_change", daily_change)
        setattr(self, "daily_diff", daily_diff)


@Depend(BenchAnalyzer)
class DrawDownAnalyzer(BaseAnalyzer):
    def __init__(self, bench_ins):
        super().__init__()
        self.__bench = bench_ins

    def _analyze(self, context: AnalyzerContext):
        """
        计算回撤序列
        """
        # benchmark = context[BenchAnalyzer].benchmark.set_index('date')
        benchmark = context[BenchAnalyzer].benchmark_with_base
        for col in benchmark.columns:
            benchmark[col] = calc_draw_down(benchmark[col].tolist())

        benchmark = bench_util.remove_net_value_base(benchmark)
        setattr(self, "draw_down", benchmark)

        total_list = benchmark.columns
        draw_down_index = pd.DataFrame()
        for symbol in total_list:
            df_temp = pd.DataFrame([[
                self.__bench.getMaxDrawdown()[symbol],
                self.__bench.getMaxDrawdowTime()[symbol],
                self.__bench.getMaxDrawdownHigh()[symbol],
                self.__bench.getMaxDrawdownLow()[symbol]
            ]], columns=[
                '最大回撤', '回撤时长', '高点时间', '低点时间'
            ], index=[symbol])
            draw_down_index = pd.concat([draw_down_index, df_temp])

        setattr(self, "draw_down_summary", draw_down_index)


@Depend(BenchAnalyzer)
class MonthlyMetricAnalyzer(BaseAnalyzer):
    @staticmethod
    def volatility(context):
        benchmark = context[BenchAnalyzer].benchmark_with_base
        # benchmark['date'] = pd.to_datetime(benchmark['date'], format='%Y%m%d')
        # benchmark.set_index('date', inplace=True)
        daily_returns = benchmark.pct_change()
        daily_returns = bench_util.remove_net_value_base(daily_returns).fillna(0)
        daily_returns.index = pd.to_datetime(daily_returns.index, format='%Y%m%d')

        # benchmark['date'] = pd.to_datetime(benchmark.index, format='%Y%m%d')
        # benchmark.set_index('date', inplace=True)

        monthly_volatility = daily_returns.resample('M').std()
        last_dt = pd.to_datetime(benchmark.index[-1], format='%Y%m%d')
        mv_index = monthly_volatility.index.tolist()
        mv_index[-1] = last_dt
        # monthly_volatility.loc[monthly_volatility.index[-1], 'index'] = last_dt
        monthly_volatility.index = mv_index
        return monthly_volatility

    def _analyze(self, context: AnalyzerContext):
        benchmark = context[BenchAnalyzer].benchmark
        benchmark['date'] = pd.to_datetime(benchmark.index, format='%Y%m%d')
        benchmark.set_index('date', inplace=True)
        benchmark_monthly = benchmark.resample('M').ffill()
        benchmark_monthly = benchmark_monthly.reset_index()
        benchmark_monthly['date'] = [x.strftime('%Y%m%d') for x in benchmark_monthly['date']]
        benchmark = benchmark.reset_index()
        benchmark['date'] = [x.strftime('%Y%m%d') for x in benchmark['date']]
        benchmark.set_index('date', inplace=True)

        """
        新增日频数据第一行到月频截面，用日频数据最后一行替换月频数据最后一行时间
        解决进行月频采样时的边界问题
        """
        bench_index_monthly = benchmark_monthly.sort_values(by='date') #.reset_index(drop=True)

        # bench_index_monthly = pd.concat([benchmark.iloc[0:1], benchmark_monthly])
        bench_index_monthly = bench_index_monthly.set_index('date')
        bench_index_monthly = bench_util.add_net_value_base(bench_index_monthly)

        bench_index_monthly.drop(bench_index_monthly.index[-1], inplace=True)
        bench_index_monthly = pd.concat([bench_index_monthly, benchmark.iloc[-1:]])
        # bench_index_monthly = bench_index_monthly.set_index('date')

        # 得出月收益率矩阵
        monthly_change = bench_index_monthly.pct_change()
        monthly_diff = bench_index_monthly.diff()
        monthly_change = monthly_change.dropna()
        monthly_diff = monthly_diff.dropna()

        bench_index_monthly = bench_util.remove_net_value_base(bench_index_monthly)
        bench_index_monthly = pd.concat([benchmark.iloc[0:1], bench_index_monthly])

        setattr(self, "benchmark_monthly", bench_index_monthly)
        setattr(self, "monthly_change", monthly_change)
        setattr(self, "monthly_diff", monthly_diff)
        setattr(self, "monthly_volatility", self.volatility(context))


@Depend(BenchAnalyzer)
class WeeklyMetricAnalyzer(BaseAnalyzer):
    """
    周频指标分析
    """
    @staticmethod
    def volatility(context):
        benchmark = context[BenchAnalyzer].benchmark_with_base
        daily_returns = benchmark.pct_change()
        daily_returns: pd.DataFrame = bench_util.remove_net_value_base(daily_returns).fillna(0)
        daily_returns.index = pd.to_datetime(daily_returns.index, format='%Y%m%d')


        weekly_volatility = daily_returns.resample('W').std()
        last_dt = pd.to_datetime(benchmark.index[-1], format='%Y%m%d')
        wv_index = weekly_volatility.index.tolist()
        wv_index[-1] = last_dt
        # monthly_volatility.loc[monthly_volatility.index[-1], 'index'] = last_dt
        weekly_volatility.index = wv_index
        return weekly_volatility

    def _analyze(self, context: AnalyzerContext):
        benchmark = context[BenchAnalyzer].benchmark
        benchmark['date'] = pd.to_datetime(benchmark.index, format='%Y%m%d')
        benchmark.set_index('date', inplace=True)
        benchmark_weekly = benchmark.resample('W').ffill()
        benchmark_weekly = benchmark_weekly.reset_index()
        benchmark_weekly['date'] = [x.strftime('%Y%m%d') for x in benchmark_weekly['date']]
        benchmark = benchmark.reset_index()
        benchmark['date'] = [x.strftime('%Y%m%d') for x in benchmark['date']]
        benchmark.set_index('date', inplace=True)

        """
        新增日频数据第一行到截面，用日频数据最后一行替换数据最后一行时间
        解决进行月样时的边界问题
        """
        bench_index_weekly = benchmark_weekly.sort_values(by='date') #.reset_index(drop=True)

        # bench_index_monthly = pd.concat([benchmark.iloc[0:1], benchmark_monthly])
        bench_index_weekly = bench_index_weekly.set_index('date')
        bench_index_weekly = bench_util.add_net_value_base(bench_index_weekly)

        bench_index_weekly.drop(bench_index_weekly.index[-1], inplace=True)
        bench_index_weekly = pd.concat([bench_index_weekly, benchmark.iloc[-1:]])
        # bench_index_monthly = bench_index_monthly.set_index('date')

        # 得出月收益率矩阵
        weekly_change = bench_index_weekly.pct_change()
        weekly_diff = bench_index_weekly.diff()
        weekly_change = weekly_change.dropna()
        weekly_diff = weekly_diff.dropna()

        bench_index_weekly = bench_util.remove_net_value_base(bench_index_weekly)
        bench_index_weekly = pd.concat([benchmark.iloc[0:1], bench_index_weekly])

        setattr(self, "benchmark_weekly", bench_index_weekly)
        setattr(self, "weekly_change", weekly_change)
        setattr(self, "weekly_diff", weekly_diff)
        setattr(self, "weekly_volatility", self.volatility(context))


@Depend(BenchAnalyzer, DailyMetricAnalyzer)
class YearlyMetricAnalyzer(BaseAnalyzer):
    @staticmethod
    def volatility(context):
        daily_returns = context[DailyMetricAnalyzer].daily_change
        daily_returns.index = pd.to_datetime(daily_returns.index, format='%Y%m%d')

        yearly_volatility = daily_returns.resample('BA').std()
        yearly_volatility['date'] = yearly_volatility.index
        yearly_volatility.loc[yearly_volatility.index[-1], 'date'] = daily_returns.index[-1]
        return yearly_volatility.set_index('date')

    def _analyze(self, context: AnalyzerContext):
        """
        对于净值列表进行年度采样，年末采样
        """
        benchmark = context[BenchAnalyzer].benchmark
        bench_index_yearly = copy.deepcopy(benchmark)

        bench_index_yearly.index = pd.to_datetime(bench_index_yearly.index) #, format='%Y%m%d')
        # bench_index_yearly.set_index('date', inplace=True)

        # TODO: 仔细检验采样resample rule的选取
        bench_index_yearly = bench_index_yearly.resample('A').ffill()
        bench_index_yearly.index = [x.strftime('%Y%m%d') for x in bench_index_yearly.index]

        bench_index_yearly = bench_util.add_net_value_base(bench_index_yearly)
        # bench_index_yearly = bench_index_yearly.reset_index()
        # bench_index_yearly['date'] = [x.strftime('%Y%m%d') for x in bench_index_yearly.index]

        # bench_index_yearly = pd.concat([bench_index_yearly, benchmark.iloc[0:1]])
        # bench_index_yearly = bench_index_yearly.sort_values(by='date').reset_index(drop=True)
        bench_index_yearly = bench_index_yearly.sort_index() # (by='date').reset_index(drop=True)
        if bench_index_yearly.index[-1] != benchmark.index[-1]:
            bench_index_yearly.drop(bench_index_yearly.index[-1], inplace=True)
            bench_index_yearly = pd.concat([bench_index_yearly, benchmark.iloc[-1:]])
        # bench_index_yearly = bench_index_yearly.set_index('date')

        # 得出年收益率矩阵
        yearly_change = bench_index_yearly.pct_change()
        yearly_change = yearly_change.dropna()
        yearly_change.index.name = 'date'

        bench_index_yearly = bench_util.remove_net_value_base(bench_index_yearly)
        bench_index_yearly = pd.concat([benchmark.iloc[0:1], bench_index_yearly])
        bench_index_yearly.index.name = 'date'

        setattr(self, "benchmark_yearly", bench_index_yearly)
        setattr(self, "yearly_change", yearly_change)
        setattr(self, "yearly_volatility", self.volatility(context))


@Depend(BenchAnalyzer, YearlyMetricAnalyzer)
class YearlyDrawDownAnalyzer(BaseAnalyzer):

    @classmethod
    def calc_draw_down(cls, context):
        benchmark = context[BenchAnalyzer].benchmark
        # benchmark['date'] = pd.to_datetime(benchmark['start_date'], format='%Y%m%d')
        benchmark['year'] = benchmark.index.map(lambda x: x[:4])
        yearly_change: pd.DataFrame = context[YearlyMetricAnalyzer].yearly_change # .reset_index()

        full_list = context[BenchAnalyzer].full_list

        def group_processor(df):
            result_dict = {
                'date': [df['year'].unique()[0]]
            }
            for name in full_list:
                max_draw_down = min(calc_draw_down(df[name].tolist(), base=1))
                result_dict[name] = [max_draw_down]
            return pd.DataFrame(result_dict)

        draw_down_result = benchmark.groupby('year').apply(group_processor).reset_index(drop=True)
        draw_down_result['date'] = yearly_change.index

        result = draw_down_result.set_index('date')
        return result

    def _analyze(self, context: AnalyzerContext):
        setattr(self, 'yearly_draw_down', self.calc_draw_down(context))


@Depend(BenchAnalyzer, MonthlyMetricAnalyzer)
class MonthlyDrawDownAnalyzer(BaseAnalyzer):
    """
    分月度统计回撤
    """
    @classmethod
    def calc_draw_down(cls, context):
        benchmark = context[BenchAnalyzer].benchmark
        # benchmark['date'] = pd.to_datetime(benchmark['start_date'], format='%Y%m%d')

        benchmark['ym'] = benchmark.index.map(lambda x: x[:6])
        monthly_change: pd.DataFrame = context[MonthlyMetricAnalyzer].monthly_change

        full_list = context[BenchAnalyzer].full_list

        def group_processor(df):
            result_dict = {
                'date': [df['ym'].unique()[0]]
            }
            for name in full_list:
                max_draw_down = min(calc_draw_down(df[name].tolist(), base=1))
                result_dict[name] = [max_draw_down]
            return pd.DataFrame(result_dict)

        draw_down_result = benchmark.groupby('ym').apply(group_processor).reset_index(drop=True)
        draw_down_result['date'] = monthly_change.index

        result = draw_down_result.set_index('date')
        return result

    def _analyze(self, context: AnalyzerContext):
        setattr(self, 'monthly_draw_down', self.calc_draw_down(context))



@Depend(BenchAnalyzer, YearlyDrawDownAnalyzer, MonthlyMetricAnalyzer, YearlyMetricAnalyzer)
class YearlyExtendedMetricAnalyzer(BaseAnalyzer):
    def calmar(self, context, draw_down_index):
        benchmark = context[BenchAnalyzer].benchmark
        # first_dt = benchmark.head().index.tolist()[0]
        benchmark_yearly = context[YearlyMetricAnalyzer].benchmark_yearly.reset_index()
        # risk_free_rate = context[BasicInfoAnalyzer].risk_free_rate
        risk_free_rate = context[BasicInfoAnalyzer].risk_free_rate

        # benchmark_yearly['start_date'] = benchmark_yearly.index.shift(1)

        yearly_change: pd.DataFrame = context[YearlyMetricAnalyzer].yearly_change.reset_index()
        start_date = pd.to_datetime(benchmark_yearly['date'].shift(1).tolist()[1:])
        end_date = pd.to_datetime(yearly_change['date'])
        yearly_change['traded_days'] = (end_date - start_date).map(lambda x: x.days + 1)

        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        total_list = [strategy_name] + bench_list + hedge_list

        for name in total_list:
            yearly_change[name] = np.power(yearly_change[name] + 1, 365.0 / yearly_change['traded_days']) - 1

        yearly_change = yearly_change.drop(columns=['traded_days']).set_index('date')

        result = - (yearly_change - risk_free_rate) / draw_down_index
        return result

    def monthly_win_ratio(self, context):
        monthly_change = context[MonthlyMetricAnalyzer].monthly_change
        monthly_change['date'] = pd.to_datetime(monthly_change.index, format='%Y%m%d')
        monthly_change.set_index('date', inplace=True)
        count_df = monthly_change.fillna(1).notna()
        win_ratio = (monthly_change > 0).resample('A').sum() / count_df.resample('A').sum()
        win_ratio.index = win_ratio.index.map(lambda x: x.strftime("%Y%m%d"))
        return win_ratio

    def tracking_error(self, context):
        daily_change = context[DailyMetricAnalyzer].daily_change
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        tracking_difference = pd.DataFrame()
        tracking_difference.index = daily_change.index
        for bench_name in bench_list:
            tracking_difference['策略对冲' + bench_name] = daily_change[strategy_name] - daily_change[bench_name]
        tracking_error = tracking_difference.resample('A').std() * np.sqrt(244)
        tracking_error[strategy_name] = 0
        tracking_error.index = tracking_error.index.map(lambda x: x.strftime("%Y%m%d"))
        return tracking_error

    def sharpe(self, context):
        benchmark_yearly = context[YearlyMetricAnalyzer].benchmark_yearly.reset_index()
        risk_free_rate = context[BasicInfoAnalyzer].risk_free_rate
        yearly_volatility = context[YearlyMetricAnalyzer].yearly_volatility.reset_index()
        benchmark_yearly['start_date'] = benchmark_yearly['date'].shift(1)

        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        yearly_change = context[YearlyMetricAnalyzer].yearly_change

        sharpe_ratio_dict = {'year': benchmark_yearly['date'].tolist()[1:]}
        total_list = [strategy_name] + bench_list + hedge_list
        start_date_series = pd.to_datetime(benchmark_yearly['start_date'], format='%Y%m%d').tolist()[1:]
        end_date_series = pd.to_datetime(benchmark_yearly['date'], format='%Y%m%d').tolist()[1:]
        for name in total_list:
            # sharpe_name = name + '夏普率'
            sharpe_ratio_dict[name] = []
            yearly_volatility_series = yearly_volatility[name]
            yearly_change_series = yearly_change[name]
            for idx_2 in range(len(start_date_series)):
                annual_volatility = yearly_volatility_series[idx_2] * math.sqrt(250)
                cum_return = yearly_change_series.iloc[idx_2]
                start_date = start_date_series[idx_2]
                end_date = end_date_series[idx_2]
                sharpe = sharpe_ratio_3(cum_return, risk_free_rate, start_date, end_date, annual_volatility)
                sharpe_ratio_dict[name].append(sharpe)

        result = pd.DataFrame(sharpe_ratio_dict)
        result = result.rename(columns={'year': '年份'}).set_index('年份')
        return result

    def reconstruct(self, context):
        """
        将数据转换为易于展示的形式
        """
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        yearly_draw_down = context[YearlyDrawDownAnalyzer].yearly_draw_down.reset_index()
        hedge_list = context[BenchAnalyzer].all_hedge_list
        yearly_change = context[YearlyMetricAnalyzer].yearly_change.reset_index()
        total_list = [strategy_name] + hedge_list
        summary = OrderedDict()
        for name in total_list:
            data = pd.DataFrame()
            data[name] = yearly_change['date']
            data['涨跌幅'] = yearly_change[name]
            data['月胜率'] = self.yearly_month_win_ratio.reset_index()[name]
            data['最大回撤'] = yearly_draw_down[name]
            data['年化夏普比'] = self.yearly_sharpe.reset_index()[name]
            data['年化卡玛比'] = self.yearly_calmar.reset_index()[name]
            data['年化跟踪误差'] = self.yearly_tracking_error.reset_index()[name]

            # 使用“策略绝对收益”代替策略名作为此项指标的表头
            if name == strategy_name:
                data.rename(columns={name: '策略绝对收益'}, inplace=True)
                summary[name] = data.set_index('策略绝对收益')
            else:
                summary[name] = data.set_index(name)

        return summary

    def _analyze(self, context: AnalyzerContext):
        """
        更为丰富的年频率绩效数据
        """
        yearly_draw_down = context[YearlyDrawDownAnalyzer].yearly_draw_down
        yearly_calmar = self.calmar(context, yearly_draw_down)
        monthly_win_ratio = self.monthly_win_ratio(context)
        tracking_error = self.tracking_error(context)
        setattr(self, 'yearly_draw_down', yearly_draw_down)
        setattr(self, 'yearly_sharpe', self.sharpe(context))
        setattr(self, 'yearly_calmar', yearly_calmar)
        setattr(self, 'yearly_month_win_ratio', monthly_win_ratio)
        setattr(self, 'yearly_tracking_error', tracking_error)
        setattr(self, 'summary', self.reconstruct(context))


@Depend(MonthlyDrawDownAnalyzer, MonthlyMetricAnalyzer)
class MonthlyAggregatedMetricAnalyzer(BaseAnalyzer):
    def monthly_aggregate(self, context):

        def monthly_agg(monthly_data):
            monthly_data['month'] = monthly_data['date'].map(lambda x: f"{int(x[4:6])}月")
            monthly_data = monthly_data.drop(columns=['date'])
            monthly_data = monthly_data.groupby('month').mean()
            return monthly_data

        monthly_draw_down = context[MonthlyDrawDownAnalyzer].monthly_draw_down.reset_index()
        setattr(self, 'monthly_agg_draw_down', monthly_agg(monthly_draw_down))

        monthly_change = context[MonthlyMetricAnalyzer].monthly_change.reset_index()
        # monthly_change['month'] = monthly_change['date'].map(lambda x: f"{int(x[4:6])}月")
        monthly_change['month'] = monthly_change['date'].map(lambda x: f"{x.month}月")
        monthly_change = monthly_change.drop(columns=['date'])
        monthly_data_mean = monthly_change.groupby('month').mean()
        monthly_data_min = monthly_change.groupby('month').min()
        monthly_data_max = monthly_change.groupby('month').max()
        monthly_data_median = monthly_change.groupby('month').median()
        count_df = monthly_change.set_index('month').fillna(1).notna().reset_index().groupby('month').sum()
        up_month = (monthly_change.set_index('month') > 0).reset_index().groupby('month').sum()
        win_ratio = (up_month / count_df)

        setattr(self, 'monthly_agg_return_mean', monthly_data_mean)
        setattr(self, 'monthly_agg_return_min', monthly_data_min)
        setattr(self, 'monthly_agg_return_max', monthly_data_max)
        setattr(self, 'monthly_agg_return_median', monthly_data_median)
        setattr(self, 'monthly_win_ration', win_ratio)



    def reconstruct(self, context):
        """
        将数据转换为易于展示的形式
        """
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        monthly_agg_draw_down = self.monthly_agg_draw_down

        hedge_list = context[BenchAnalyzer].all_hedge_list
        total_list = [strategy_name] + hedge_list
        summary = OrderedDict()
        for name in total_list:
            data = pd.DataFrame()
            data[name] = monthly_agg_draw_down.index
            data['涨跌幅均值'] = self.monthly_agg_return_mean.reset_index()[name]
            data['涨跌幅最小值'] = self.monthly_agg_return_min.reset_index()[name]
            data['涨跌幅最大值'] = self.monthly_agg_return_max.reset_index()[name]
            data['涨跌幅中位数'] = self.monthly_agg_return_median.reset_index()[name]
            data['胜率'] = self.monthly_win_ration.reset_index()[name]
            data['最大回撤'] = monthly_agg_draw_down.reset_index()[name]
            # data['年化夏普比'] = self.yearly_sharpe.reset_index()[name]
            # data['年化卡玛比'] = self.yearly_calmar.reset_index()[name]
            # data['追踪误差'] = self.yearly_tracking_error.reset_index()[name]
            data['sort_idx'] = data[name].map(lambda x: int(x[:-1]))
            data = data.sort_values(['sort_idx']).drop(columns=['sort_idx'])

            # 使用“策略绝对收益”代替策略名作为此项指标的表头
            if name == strategy_name:
                data.rename(columns={name: '策略绝对收益'}, inplace=True)
                summary[name] = data.set_index('策略绝对收益')
            else:
                summary[name] = data.set_index(name)

        return summary

    def _analyze(self, context: AnalyzerContext):
        self.monthly_aggregate(context)
        setattr(self, 'summary', self.reconstruct(context))


@Depend(BasicInfoAnalyzer, BenchAnalyzer, DailyMetricAnalyzer)
class BetaAnalyzer(BaseAnalyzer):
    def _analyze(self, context):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        daily_change = context[DailyMetricAnalyzer].daily_change
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list

        beta_dict = {}
        # for symbol in self.__bench.getNameList():
        for symbol in bench_list:
            v = daily_change[symbol].var()
            if np.isnan(v) or v == 0:
                beta_dict[symbol] = '--'
            else:
                beta_dict[symbol] = daily_change[symbol].cov(daily_change[strategy_name]) / daily_change[symbol].var()

        beta_dict[strategy_name] = '--'
        for symbol in hedge_list:
            beta_dict[symbol] = '--'

        setattr(self, "beta_dict", beta_dict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, BetaAnalyzer)
class AlphaAnalyzer(BaseAnalyzer):
    """
    计算alpha值
    alpha = R_p - (R_f + beta *(R_m - R_f))
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        risk_free_rate = context[BasicInfoAnalyzer].risk_free_rate
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        annual_return = context[BenchAnalyzer].annual_return
        beta_dict = context[BetaAnalyzer].beta_dict

        alpha_dict = {}
        # for symbol in self.__bench.getNameList():
        for symbol in bench_list:
            if beta_dict[symbol] != '--':
                # alphaDict[symbol] = annual_return - (self.__riskFreeRate + betaDict[symbol]*(self.__bench.getAnnualReturn()[symbol] - self.__riskFreeRate))
                alpha_dict[symbol] = annual_return[strategy_name] - \
                                     (risk_free_rate + beta_dict[symbol] * (annual_return[symbol] - risk_free_rate))
            else:
                alpha_dict[symbol] = '--'
        # print alphaDict
        alpha_dict[strategy_name] = '--'
        # for symbol in self.__bench.getHedgeList():
        for symbol in hedge_list:
            alpha_dict[symbol] = '--'

        setattr(self, "alpha_dict", alpha_dict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, DailyMetricAnalyzer)
class InfoRatioAnalyzer(BaseAnalyzer):
    """
    计算信息比率  (策略年化收益率 - 基准年化收益率) / (策略与基准每日收益差值的年化标准差)
    """

    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        daily_change = context[DailyMetricAnalyzer].daily_change
        annual_return = context[BenchAnalyzer].annual_return
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list

        info_ratio = {}
        # for symbol in self.__bench.getNameList():
        for symbol in bench_list:
            # returnDiff = annual_return - self.__bench.getAnnualReturn()[symbol]
            return_diff = annual_return[strategy_name] - annual_return[symbol]
            return_diff_list = daily_change[strategy_name] - daily_change[symbol]
            diff_std = (return_diff_list.std()) * (math.sqrt(250))

            if diff_std != 0:
                info_ratio[symbol] = return_diff / diff_std
            else:
                info_ratio[symbol] = '--'
        # print infoRatio
        info_ratio[strategy_name] = '--'
        # for symbol in self.__bench.getHedgeList():
        for symbol in hedge_list:
            info_ratio[symbol] = '--'

        setattr(self, "info_ratio", info_ratio)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, DailyMetricAnalyzer)
class DailyWinRatioAnalyzer(BaseAnalyzer):
    """
    日胜率统计
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        name_list = context[BenchAnalyzer].name_list
        hedge_list = context[BenchAnalyzer].hedge_list
        daily_change = context[DailyMetricAnalyzer].daily_change

        daily_win_ratio_dict = {}

        strategy_daily_win_count = daily_change[daily_change[strategy_name] >= 0][strategy_name].count()
        strategy_daily_lose_count = daily_change[daily_change[strategy_name] < 0][strategy_name].count()


        if strategy_daily_lose_count !=0:
            daily_win_ratio_dict[strategy_name] = strategy_daily_win_count/float(strategy_daily_lose_count)
        else:
            daily_win_ratio_dict[strategy_name] = u'全胜'

        for symbol in name_list:
            relative_daily_win_count =  daily_change[daily_change[strategy_name] >= daily_change[symbol]][strategy_name].count()
            relative_daily_lose_count = daily_change[daily_change[strategy_name] < daily_change[symbol]][strategy_name].count()
            if relative_daily_lose_count != 0:
                daily_win_ratio_dict[symbol] = relative_daily_win_count/float(relative_daily_lose_count)
            else:
                daily_win_ratio_dict[symbol] = u'全胜'
        for symbol in hedge_list:
            daily_win_ratio_dict[symbol] = u'--'

        setattr(self, "daily_win_ratio_dict", daily_win_ratio_dict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, WeeklyMetricAnalyzer)
class WeeklyWinRatioAnalyzer(BaseAnalyzer):
    """
    周频率统计，绝对胜率
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        weekly_change = context[WeeklyMetricAnalyzer].weekly_change

        weekly_win_ratio_dict = {}
        # totalList = [self.__strategyName] + self.__bench.getNameList() + self.__bench.getHedgeList()
        total_list = [strategy_name] + bench_list + hedge_list
        for symbol in total_list:
            strategy_weekly_win_count = weekly_change[weekly_change[symbol] >= 0][symbol].count()
            strategy_weekly_lose_count = weekly_change[weekly_change[symbol] < 0][symbol].count()

            """
            print symbol
            print strategyMonthlyWinCount
            print strategyMonthlyLoseCount
            """
            if strategy_weekly_lose_count != 0:
                weekly_win_ratio_dict[symbol] = strategy_weekly_win_count / float(
                    strategy_weekly_lose_count + strategy_weekly_win_count)
            else:
                weekly_win_ratio_dict[symbol] = '100%'

        setattr(self, "weekly_win_ratio_dict", weekly_win_ratio_dict)



@Depend(BasicInfoAnalyzer, BenchAnalyzer, MonthlyMetricAnalyzer)
class MonthlyWinRatioAnalyzer(BaseAnalyzer):
    """
    月胜率统计，绝对胜率
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        monthly_change = context[MonthlyMetricAnalyzer].monthly_change

        monthly_win_ratio_dict = {}
        # totalList = [self.__strategyName] + self.__bench.getNameList() + self.__bench.getHedgeList()
        total_list = [strategy_name] + bench_list + hedge_list
        for symbol in total_list:
            strategyMonthlyWinCount = monthly_change[monthly_change[symbol] >= 0][symbol].count()
            strategyMonthlyLoseCount = monthly_change[monthly_change[symbol] < 0][symbol].count()

            """
            print symbol
            print strategyMonthlyWinCount
            print strategyMonthlyLoseCount
            """
            if strategyMonthlyLoseCount != 0:
                monthly_win_ratio_dict[symbol] = strategyMonthlyWinCount / float(
                    strategyMonthlyLoseCount + strategyMonthlyWinCount)
            else:
                monthly_win_ratio_dict[symbol] = '100%'

        setattr(self, "monthly_win_ratio_dict", monthly_win_ratio_dict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, MonthlyMetricAnalyzer)
class MonthlyRelativeWinRatioAnalyzer(BaseAnalyzer):
    """
    月胜率统计，策略相对基准月胜率
    """
    def _analyze(self, context: AnalyzerContext):

        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        monthlyChange = context[MonthlyMetricAnalyzer].monthly_change
        monthlyRelativeWinRatioDict = {}

        # for symbol in self.__bench.getNameList():
        for symbol in bench_list:
            # print symbol
            relativeMonthlyWinCount = monthlyChange[monthlyChange[strategy_name] >= monthlyChange[symbol]][
                strategy_name].count()
            relativeMonthlyLoseCount = monthlyChange[monthlyChange[strategy_name] < monthlyChange[symbol]][
                strategy_name].count()
            """
            print symbol
            print monthlyChange[monthlyChange[strategyName] >= monthlyChange[symbol]]
            print monthlyChange[monthlyChange[strategyName] < monthlyChange[symbol]]
            print relativeMonthlyWinCount 
            print relativeMonthlyLoseCount
            """
            if relativeMonthlyLoseCount != 0:
                monthlyRelativeWinRatioDict[symbol] = relativeMonthlyWinCount / float(
                    relativeMonthlyLoseCount + relativeMonthlyWinCount)
            else:
                monthlyRelativeWinRatioDict[symbol] = '100%'
        # for symbol in self.__bench.getHedgeList():
        for symbol in hedge_list:
            monthlyRelativeWinRatioDict[symbol] = '--'
        monthlyRelativeWinRatioDict[strategy_name] = '--'

        setattr(self, "monthly_relative_win_ratio_dict", monthlyRelativeWinRatioDict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, MonthlyMetricAnalyzer)
class MonthlyRelativeWinRatioUpAnalyzer(BaseAnalyzer):
    """
    相对月胜率统计,基准上涨时相对月胜率
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        monthlyChange = context[MonthlyMetricAnalyzer].monthly_change

        monthly_relative_win_ratio_up_dict = {}
        # for symbol in self.__bench.getNameList():
        for symbol in bench_list:
            # print symbol
            monthlyChangeUp = monthlyChange[monthlyChange[symbol] >= 0]
            relativeMonthlyWinCount = monthlyChangeUp[monthlyChangeUp[strategy_name] >= monthlyChangeUp[symbol]][
                strategy_name].count()
            relativeMonthlyLoseCount = monthlyChangeUp[monthlyChangeUp[strategy_name] < monthlyChangeUp[symbol]][
                strategy_name].count()
            """
            print symbol
            print monthlyChange[monthlyChange[strategyName] >= monthlyChange[symbol]]
            print monthlyChange[monthlyChange[strategyName] < monthlyChange[symbol]]
            print relativeMonthlyWinCount 
            print relativeMonthlyLoseCount
            """
            if relativeMonthlyLoseCount != 0:
                monthly_relative_win_ratio_up_dict[symbol] = relativeMonthlyWinCount / float(
                    relativeMonthlyLoseCount + relativeMonthlyWinCount)
            else:
                monthly_relative_win_ratio_up_dict[symbol] = '100%'
        # f or symbol in self.__bench.getHedgeList():
        for symbol in hedge_list:
            monthly_relative_win_ratio_up_dict[symbol] = '--'
        monthly_relative_win_ratio_up_dict[strategy_name] = '--'

        setattr(self, "monthly_relative_win_ratio_up_dict", monthly_relative_win_ratio_up_dict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, MonthlyMetricAnalyzer)
class MonthlyRelativeWinRatioDownAnalyzer(BaseAnalyzer):
    """
    相对月胜率统计,基准下跌时相对月胜率
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        monthlyChange = context[MonthlyMetricAnalyzer].monthly_change

        monthly_relative_win_ratio_down_dict = {}
        # for symbol in self.__bench.getNameList():
        for symbol in bench_list:
            # print symbol
            monthlyChangeDown = monthlyChange[monthlyChange[symbol] < 0]
            relativeMonthlyWinCount = \
                monthlyChangeDown[monthlyChangeDown[strategy_name] >= monthlyChangeDown[symbol]][
                    strategy_name].count()
            relativeMonthlyLoseCount = \
                monthlyChangeDown[monthlyChangeDown[strategy_name] < monthlyChangeDown[symbol]][
                    strategy_name].count()
            """
            print symbol
            print monthlyChange[monthlyChange[strategyName] >= monthlyChange[symbol]]
            print monthlyChange[monthlyChange[strategyName] < monthlyChange[symbol]]
            print relativeMonthlyWinCount 
            print relativeMonthlyLoseCount
            """
            if relativeMonthlyLoseCount != 0:
                monthly_relative_win_ratio_down_dict[symbol] = relativeMonthlyWinCount / float(
                    relativeMonthlyLoseCount + relativeMonthlyWinCount)
            else:
                monthly_relative_win_ratio_down_dict[symbol] = '100%'
        # for symbol in self.__bench.getHedgeList():
        for symbol in hedge_list:
            monthly_relative_win_ratio_down_dict[symbol] = '--'
        monthly_relative_win_ratio_down_dict[strategy_name] = '--'

        setattr(self, "monthly_relative_win_ratio_down_dict", monthly_relative_win_ratio_down_dict)


@Depend(BasicInfoAnalyzer, BenchAnalyzer, DailyMetricAnalyzer)
class WinLoseAmountRatioAnalyzer(BaseAnalyzer):
    """
    盈亏比计算
    """
    def _analyze(self, context: AnalyzerContext):
        strategy_name = context[BasicInfoAnalyzer].strategy_name
        bench_list = context[BenchAnalyzer].all_bench_list
        hedge_list = context[BenchAnalyzer].all_hedge_list
        dailyDiff = context[DailyMetricAnalyzer].daily_diff

        win_lose_amount_ratio_dict = {}
        # totalList = [self.__strategyName] + self.__bench.getNameList() + self.__bench.getHedgeList()
        totalList = [strategy_name] + bench_list + hedge_list
        for symbol in totalList:
            winAmount = dailyDiff[dailyDiff[symbol] >= 0][symbol].sum()
            loseAmount = dailyDiff[dailyDiff[symbol] < 0][symbol].sum()
            """
            print winAmount
            print loseAmount
            """
            if loseAmount != 0:
                win_lose_amount_ratio_dict[symbol] = abs(winAmount / float(loseAmount))
            else:
                win_lose_amount_ratio_dict[symbol] = '100%'

        setattr(self, "win_lose_amount_ratio_dict", win_lose_amount_ratio_dict)


@Depend(InfoRatioAnalyzer, BetaAnalyzer, AlphaAnalyzer, MonthlyWinRatioAnalyzer, MonthlyRelativeWinRatioAnalyzer,
        MonthlyRelativeWinRatioUpAnalyzer, MonthlyRelativeWinRatioDownAnalyzer, WinLoseAmountRatioAnalyzer)
class MetricSummaryAnalyzer(BaseAnalyzer):
    def __init__(self, bench_ins):
        super().__init__()
        self.__bench = bench_ins

    def _analyze(self, context: AnalyzerContext):
        benchmark = context[BenchAnalyzer].benchmark #.set_index('date')
        total_list = benchmark.columns
        # totalList = [self.__strategyName] + self.__bench.getAllBenchList() + self.__bench.getAllHedgeList()
        strategy_metric = pd.DataFrame()
        for symbol in total_list:
            df_temp = pd.DataFrame([[
                self.__bench.getCumulativeReturn()[symbol],
                self.__bench.getAnnualReturn()[symbol],
                self.__bench.getVolatility()[symbol],
                self.__bench.getMaxDrawdown()[symbol],
                self.__bench.getSharpeRatio()[symbol],
                context[InfoRatioAnalyzer].info_ratio[symbol],
                context[BetaAnalyzer].beta_dict[symbol],
                context[AlphaAnalyzer].alpha_dict[symbol],
                context[MonthlyWinRatioAnalyzer].monthly_win_ratio_dict[symbol],
                context[MonthlyRelativeWinRatioAnalyzer].monthly_relative_win_ratio_dict[symbol],
                context[MonthlyRelativeWinRatioUpAnalyzer].monthly_relative_win_ratio_up_dict[symbol],
                context[MonthlyRelativeWinRatioDownAnalyzer].monthly_relative_win_ratio_down_dict[symbol],
                context[WinLoseAmountRatioAnalyzer].win_lose_amount_ratio_dict[symbol]
            ]], columns=[
                '策略收益', '年化收益率', '波动率', '最大回撤', '夏普比率', '信息比率',
                '贝塔', '阿尔法', '月胜率', '相对基准月胜率', '基准上涨相对月胜率', '基准下跌相对月胜率', '盈亏比'
            ], index=[symbol])
            strategy_metric = pd.concat([strategy_metric, df_temp])

        setattr(self, 'metric_summary', strategy_metric)