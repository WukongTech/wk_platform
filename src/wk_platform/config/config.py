"""

"""
from __future__ import annotations

from wk_platform.broker.commission import TradePercentage, NoCommission
from wk_platform.broker.commission import TradePercentageTaxFee, NoCommissionTaxFee, StampTaxAShare
from wk_platform import __version__
import wk_util.logger

from wk_platform.config.enums import *


def assert_positive_or_none(v):
    if v is not None:
        assert v > 0


def assert_negative_or_none(v):
    if v is not None:
        assert v < 0


class MetricConfiguration:
    def __init__(self,
                 capacity_proportion=0.1,
                 capacity_quantile=0.25,
                 tracking_transaction=True,
                 detailed_position_track_level=TrackLevel.TRADE_DAY,
                 position_track_level=TrackLevel.TRADE_DAY
                 ):
        pass


class StrategyConfiguration:
    def __init__(self,
                 strategy_name="myStrategy",
                 datasets=('a_share_market', ),
                 initial_cash=1e8,
                 volume_limit=None,
                 max_up_down_limit: MaxUpDownType | str = MaxUpDownType.STRICT,
                 suspension_limit=True,
                 commission=0.0003,
                 stamp_tax=0.001,
                 max_position=1,
                 risk_free_rate=0,
                 trade_rule='t+1',
                 whole_batch_only=False,
                 progress_bar=True,
                 price_with_commission=False,
                 adapt_quantity=True,
                 price_type: PriceType | str = PriceType.OPEN,
                 position_cost_type: PositionCostType | str = PositionCostType.TRADE_DATE,
                 broker=None,
                 stop_profit=None,
                 stop_loss=None,
                 intraday_stop_profit=None,
                 intraday_stop_loss=None,
                 # stop_pnl_replacement=None,
                 allow_synth_etf=False,
                 capacity_proportion=0.1,
                 capacity_quantile=0.25,
                 tracking_transaction=True,
                 detailed_position_track_level: TrackLevel | str = TrackLevel.TRADE_DAY,
                 position_track_level: TrackLevel | str =TrackLevel.TRADE_DAY,
                 calendar='a_share_market',
                 profile_runtime=True):
        """
        Parameters
        ==================
        strategy_name: str
            策略名称
        datasets: tuple(str)
            回测时使用的数据集，默认只包含日行情数据
        initial_cash: float
            初始资金，默认1e8
        volume_limit: float, None
            是否开启交易量限制，取值为0~1，代表占当天交易量的比例，取值为None时无限制，默认关闭
        max_up_down_limit: MaxUpDownType
            存在涨跌停情况时的处理规则
        suspension_limit: bool
            是否开启停复牌限制，默认开启
        commission: float
            券商佣金，目前仅支持按百分比设置，默认0.0003
        stamp_tax: float | str
            印花税率，可以使用数值指定，或根据市场指定
        max_position: float
            最大持仓比例，权重表中权重之和低于仓位时，按实际权重确定持仓；高于仓位时，按总权重进行归一化后确定其在最大持仓部分的比例
        risk_free_rate: float
            无风险利率，用于计算夏普比等指标，默认为0
        trade_rule: str
            交易规则，默认t+1
        whole_batch_only: bool
            是否限制仅允许整手买入，默认不做限制
        price_with_commission: bool
            根据持仓权重计算交易量时，是否将佣金作为价格的一部分用于计算实际交易数量。
            仅适用于根据百分比收取佣金的情形，仅对买入操作生效
        adapt_quantity: bool
            在现金不足时是否根据当前现金调整交易数量。通常在每次调仓的最后一笔交易中出现
        price_type: PriceType
            交易时使用的价格类型，默认为开盘价
        progress_bar: bool
            是否显示加载feed和策略运行的进度条
        broker: object
            使用的broker
        stop_profit: float, None
            止盈涨幅，None表示不进行止盈操作
        stop_loss: float, None
            止损跌幅，None表示不进行止损操作。例如亏损10%时止损，设定stop_loss=-0.1
        stop_pnl_replacement: str, None
            止盈/止损后用于填充空仓位的标的，使用wind代码表示。若使用指数代码则用对应的模拟指数ETF填充。None表示止盈/止损后保持空仓位
        allow_synth_etf: bool
            是否允许买入卖出指数模拟ETF
        position_cost_type: PositionCostType
            持仓成本计算方法，默认根据最新换仓日的价格计算
        capacity_proportion: float
            容量估算参数，对应策略容量估算公式中的beta，默认0.1
        capacity_quantile: float
            容量估算参数，对应策略容量估算时所取的分位点，默认0.25
        tracking_transaction: bool
            是否开启交易流水追踪，默认开启
        detailed_position_track_level: TrackLevel
            详细持仓的记录级别
        position_track_level: TrackLevel
            持仓记录级别，默认记录调仓日
        calendar: str
            回测使用的日历，默认a_share
        profile_runtime: bool
            是否追踪运行性能
        """

        if isinstance(max_up_down_limit, str):
            max_up_down_limit = MaxUpDownType[max_up_down_limit.upper()]
        if isinstance(price_type, str):
            price_type = PriceType[price_type.upper()]
        if isinstance(position_cost_type, str):
            position_cost_type = PositionCostType[position_cost_type.upper()]
        if isinstance(detailed_position_track_level, str):
            detailed_position_track_level = TrackLevel[detailed_position_track_level.upper()]
        if isinstance(position_track_level, str):
            position_track_level = TrackLevel[position_track_level.upper()]

        if isinstance(calendar, str):
            calendar = CalendarType[calendar.upper()]

        self.__strategy_name = strategy_name

        self.__datasets = datasets

        self.__init_cash = initial_cash

        assert volume_limit is None or 0 < volume_limit <= 1
        self.__volume_limit = volume_limit

        self.__max_up_down_limit = max_up_down_limit
        self.__suspension_limit = suspension_limit
        self.__progress_bar = progress_bar if wk_util.logger.SHOW_RUNTIME_INFO else False
        self.__broker = broker

        assert 0 < max_position <= 1
        self.__max_position = max_position

        assert isinstance(commission, float) or isinstance(commission, int)
        assert isinstance(stamp_tax, float) or isinstance(stamp_tax, int) or isinstance(stamp_tax, str)

        if isinstance(stamp_tax, str):
            stamp_tax = stamp_tax.lower()
            assert stamp_tax in ( 'a_share', )

        self.__price_with_commission = price_with_commission

        self.__price_type = price_type

        self.__adapt_quantity = adapt_quantity

        if commission == 0:
            self.__commission = NoCommission()
        else:
            self.__commission = TradePercentage(commission)

        if stamp_tax == 0:
            self.__stamp_tax = NoCommissionTaxFee()
        elif isinstance(stamp_tax, str):
            self.__stamp_tax = StampTaxAShare()
        else:
            self.__stamp_tax = TradePercentageTaxFee(stamp_tax)
        self.__whole_batch_only = whole_batch_only

        # assert isinstance(risk_free_rate, float)
        self.__risk_free_rate = risk_free_rate

        assert trade_rule in ('t+0', 't+1')
        self.__trade_rule = TradeRule.T1 if trade_rule == 't+1' else TradeRule.T0

        assert_positive_or_none(stop_profit)
        assert_negative_or_none(stop_loss)

        self.__stop_profit = stop_profit
        self.__stop_loss = stop_loss

        self.__stop_pnl = bool(stop_profit) or bool(stop_loss)

        assert_positive_or_none(intraday_stop_profit)
        assert_negative_or_none(intraday_stop_loss)
        self.__intraday_stop_profit = intraday_stop_profit
        self.__intraday_stop_loss = intraday_stop_loss

        self.__intraday_stop_pnl = bool(intraday_stop_profit) or bool(intraday_stop_loss)

        # self.__stop_pnl_replacement = stop_pnl_replacement

        if self.__stop_pnl and not (position_cost_type == PositionCostType.TRADE_DATE):
            raise AttributeError('`position_cost_type` should be `PositionCostType.TRADE_DATE` '
                                 'when using stop profit/loss operation')

        self.__allow_synth_etf = allow_synth_etf

        self.__position_cost_type = position_cost_type

        self.__detailed_position_track_level = detailed_position_track_level

        self.__position_track_level = position_track_level

        self.__tracking_transaction = tracking_transaction

        self.__capacity_proportion = capacity_proportion
        self.__capacity_quantile = capacity_quantile

        self.__calendar = calendar

        self.__profile_runtime = profile_runtime

        self.__version = __version__

    @property
    def version(self):
        return self.__version

    @property
    def type(self):
        return "股票多头"

    @property
    def strategy_name(self):
        return self.__strategy_name

    @property
    def datasets(self):
        return self.__datasets

    @property
    def initial_cash(self):
        return self.__init_cash

    @property
    def volume_limit(self):
        return self.__volume_limit

    @property
    def max_up_down_limit(self):
        return self.__max_up_down_limit

    @property
    def suspension_limit(self):
        return self.__suspension_limit

    @property
    def commission(self):
        return self.__commission

    @property
    def stamp_tax(self):
        return self.__stamp_tax

    @property
    def max_position(self):
        return self.__max_position

    @property
    def whole_batch_only(self):
        return self.__whole_batch_only

    @property
    def price_with_commission(self):
        return self.__price_with_commission

    @property
    def price_type(self):
        return self.__price_type

    @property
    def progress_bar(self):
        return self.__progress_bar

    @property
    def risk_free_rate(self):
        return self.__risk_free_rate

    @property
    def trade_rule(self):
        return self.__trade_rule

    @property
    def adapt_quantity(self):
        return self.__adapt_quantity

    @property
    def using_broker(self):
        return self.__broker is not None

    @property
    def broker(self):
        return self.__broker

    @property
    def position_cost_type(self):
        return self.__position_cost_type

    @property
    def stop_profit(self):
        return self.__stop_profit

    @property
    def stop_loss(self):
        return self.__stop_loss

    @property
    def stop_pnl(self):
        return self.__stop_pnl

    @property
    def intraday_stop_profit(self):
        return self.__intraday_stop_profit

    @property
    def intraday_stop_loss(self):
        return self.__intraday_stop_loss

    @property
    def intraday_stop_pnl(self):
        return self.__intraday_stop_pnl

    # @property
    # def stop_pnl_replacement(self):
    #     return self.__stop_pnl_replacement

    @property
    def allow_synth_etf(self):
        return self.__allow_synth_etf

    @property
    def capacity_proportion(self):
        return self.__capacity_proportion

    @property
    def capacity_quantile(self):
        return self.__capacity_quantile

    @property
    def detailed_position_track_level(self):
        return self.__detailed_position_track_level

    @property
    def position_track_level(self):
        return self.__position_track_level

    @property
    def tracking_transaction(self):
        return self.__tracking_transaction

    @property
    def calendar(self):
        return self.__calendar

    @property
    def profile_runtime(self):
        return self.__profile_runtime


# class StrategyConfiguration:
#     def __init__(self, *args, **kwargs):
#         raise ImportError("import `StrategyConfiguration` from `wk_platform.config` is deprecated.")


class HedgeStrategyConfiguration(StrategyConfiguration):
    def __init__(self,
                 initial_cash=1e8,
                 volume_limit=None,
                 max_up_down_limit=MaxUpDownType.STRICT,
                 suspension_limit=True,
                 commission=0.0003,
                 stamp_tax=0.001,
                 code='IC',
                 stock_position=0.8,
                 deposit=0.15,
                 deposit_cash_ratio=0.7,
                 max_hedge_deviation=0.08,
                 hedge_ratio=1,
                 future_commission=0.00023,
                 risk_free_rate=0,
                 trade_rule='t+1',
                 whole_batch_only=False,
                 progress_bar=True,
                 price_with_commission=False,
                 adapt_quantity=True,
                 price_type=PriceType.OPEN,
                 position_cost_type=PositionCostType.TRADE_DATE,
                 broker=None,
                 stop_profit=None,
                 stop_loss=None,
                 tracking_transaction=True,
                 detailed_position_track_level=TrackLevel.TRADE_DAY,
                 position_track_level=TrackLevel.TRADE_DAY):
        """
        Parameters
        ==================
        initial_cash: float
            初始资金，默认1e8
        volume_limit: float, None
            是否开启交易量限制，取值为0~1，代表占当天交易量的比例，取值为None时无限制，默认关闭
        max_up_down_limit: MaxUpDownType
            存在涨跌停情况时的处理规则
        suspension_limit: bool
            是否开启停复牌限制，默认开启
        commission: float
            券商佣金，目前仅支持按百分比设置，默认0.0003
        stamp_tax: float
            印花税率，默认0.001
        code: str
            用于对冲的股指期货代码
        stock_position: float
            股票仓位
        deposit: float
            保证金比例
        deposit_cash_ratio: float
            保证金占总现金的比例，超过该比例时，会触发强制调仓
        max_hedge_deviation: float
            对冲比率偏离度，如果对冲比率与目标比率的偏差超过此阈值，进行仓位重平衡操作。特别地，当对冲比率为1时，此参数等价为最大头寸暴露，
            即如果股票价值与期货价值超过此阈值，进行期货仓位重平衡操作
        hedge_ratio: float
            对冲比率，即期货与股票市值的目标比率
        future_commission: float
            期货交易手续费
        risk_free_rate: float
            无风险利率，用于计算夏普比等指标，默认为0
        trade_rule: str
            交易规则，默认t+1
        whole_batch_only: bool
            是否限制仅允许整手买入，默认不做限制
        price_with_commission: bool
            根据持仓权重计算交易量时，是否将佣金作为价格的一部分用于计算实际交易数量。
            仅适用于根据百分比收取佣金的情形，仅对买入操作生效
        adapt_quantity: bool
            在现金不足时是否根据当前现金调整交易数量。通常在每次调仓的最后一笔交易中出现
        price_type: PriceType
            交易时使用的价格类型，默认为开盘价
        progress_bar: bool
            是否显示加载feed和策略运行的进度条
        broker: object
            使用的broker
        stop_profit: float, None
            止盈涨幅，None表示不进行止盈操作
        stop_loss: float, None
            止损跌幅，None表示不进行止损操作
        position_cost_type: PositionCostType
            持仓成本计算方法，默认根据最新换仓日的价格计算
        tracking_transaction: bool
            是否开启交易流水追踪，默认开启
        detailed_position_track_level: TrackLevel
            详细持仓的记录级别
        position_track_level: TrackLevel
            持仓记录级别，默认记录调仓日
        """
        self.__stock_position = stock_position
        self.__deposit = deposit
        self.__hedge_ratio = hedge_ratio
        self.__future_commission = future_commission
        self.__max_hedge_deviation = max_hedge_deviation
        self.__code = code
        self.__d_c_ratio = deposit_cash_ratio
        super().__init__(
            initial_cash=initial_cash,
            volume_limit=volume_limit,
            max_up_down_limit=max_up_down_limit,
            suspension_limit=suspension_limit,
            commission=commission,
            stamp_tax=stamp_tax,
            risk_free_rate=risk_free_rate,
            trade_rule=trade_rule,
            whole_batch_only=whole_batch_only,
            progress_bar=progress_bar,
            price_with_commission=price_with_commission,
            adapt_quantity=adapt_quantity,
            price_type=price_type,
            position_cost_type=position_cost_type,
            broker=broker,
            stop_profit=stop_profit,
            stop_loss=stop_loss,
            tracking_transaction=tracking_transaction,
            detailed_position_track_level=detailed_position_track_level,
            position_track_level=position_track_level)

    @property
    def type(self):
        return "股票多头对冲股指期货"

    @property
    def stock_position(self):
        return self.__stock_position

    @property
    def deposit(self):
        return self.__deposit

    @property
    def hedge_ratio(self):
        return self.__hedge_ratio

    @property
    def future_commission(self):
        return self.__future_commission

    @property
    def max_hedge_deviation(self):
        return self.__max_hedge_deviation

    @property
    def deposit_cash_ratio(self):
        return self.__d_c_ratio

    @property
    def code(self):
        return self.__code


