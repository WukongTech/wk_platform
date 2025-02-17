import logging
from collections import deque
# logging.basicConfig(level=logging.ERROR)
import pathlib

import pandas as pd
import datetime
import time
from datetime import timedelta
from collections import OrderedDict


from wk_platform import strategy
from wk_platform import __version__
from wk_platform.backtest import strategyOutput
from wk_platform.config import HedgeStrategyConfiguration
from wk_platform.util.future import FutureUtil

from wk_data.data_source import DataSource
from wk_platform.backtest.result import BackTestResult, BackTestResultSet
from wk_data.constants import ExtStatus, SuspensionType
from wk_util.logger import console_log
from wk_util.file_digest import md5
from wk_platform.feed.fast_feed import StockFutureFeed
from wk_platform.broker.brokers import HedgeBroker
import wk_util.logger
import wk_db
from wk_platform.contrib.util import check_weight_df


class HedgeStrategyBase(strategy.BacktestingStrategy):
    def __init__(self, feed, data, begin, end, config=HedgeStrategyConfiguration(), ext_status_data=None, mr_map=None,
                 sign=None):
        """
        初始化策略父类，设定回测起始资金
        """
        super().__init__(feed, HedgeBroker, config)

        self.__ext_date = None
        self.__config: HedgeStrategyConfiguration = config

        """
        记录策略开始和结束时间
        """
        self.__begin_date = begin
        self.__end_date = end

        """
        记录股票权重数据
        """
        self.__weight = data

        """
        记录调仓日期
        """
        self.__buyDate = []

        self.__target_positions = {}

        self.__trade_date = deque()

        self.__ext_status_data = deque(ext_status_data) if ext_status_data is not None else deque()
        self.__mr_map = mr_map if mr_map is not None else {}

        # print('mr_map')
        # for k,v in self.__mr_map.items():
        #     print(k, v)

        self.__pbar = None
        self.__sign = sign
        self.getBroker().resubscribe()  # 重构订单逻辑，此处暂时不做处理

        self.__current_contract = FutureUtil.current_contract(self.__config.code,
                                                              datetime.datetime.strptime(self.__begin_date, "%Y%m%d"))
        self.__current_bars = None

    @property
    def begin_date(self):
        return self.__begin_date

    @property
    def end_date(self):
        return self.__end_date

    @property
    def sign(self):
        return self.__sign

    def get_trade_date(self):
        return self.__buyDate

    # def calc_future_quantity(self, bar, cash_amount):
    #     """
    #     计算输入金额可以买入的股票数目
    #     """
    #     price = bar.get_price(self.__config.price_type)
    #     quantity = cash_amount // (price * 300) * 300
    #
    #     return quantity

    def enter_future_long_short_weight(self, bars, instrument, weight, good_till_canceled=False, all_or_none=False):
        """
        获取当前总资产，按照开盘价计算
        """
        total_equity = self.getBroker().get_total_equity(self.__config.price_type)

        """
        计算原来持仓市值，按照开盘价计算
        """
        position = self.getBroker().getFuturePosition(instrument)
        if position is None:
            old_quantity = 0
        else:
            old_quantity = position.quantity

        """
        计算此次要目标市值
        """
        target_amount = total_equity * weight
        price = bars[instrument].get_price(self.__config.price_type)
        target_quantity = FutureUtil.calc_future_quantity(target_amount, price)

        quantity = target_quantity - old_quantity

        if quantity is None:
            # 暂时使用多头仓位代替
            return strategy.position.DummyPosition(self, bars, instrument, None, None, quantity,
                                                   good_till_canceled, all_or_none)

        """
        执行买入或者卖出
        """
        if quantity > 0:
            quantity = self.adjusted_shares(quantity)
            if quantity > 0:
                return strategy.position.LongPosition(self, bars, instrument, None, None, quantity,
                                                      good_till_canceled, all_or_none)
            else:
                return None
        elif quantity < 0:
            quantity = abs(quantity)
            # 注意处理整手交易的情况
            if quantity > 0:
                return strategy.position.ShortPosition(self, bars, instrument, None, None, abs(quantity),
                                                       good_till_canceled, all_or_none)
        else:
            return None

    def __change_position(self, bars):
        date_str = bars.getDateTime().strftime("%Y%m%d")
        weightPerTrade = self.__target_positions.get_group(date_str)
        weightPerTrade = weightPerTrade[weightPerTrade["windcode"].isin(bars.getInstruments())]


        weightPerTrade = weightPerTrade.set_index('date')

        """
        获取了当期要买入的股票列表和权重列表
        按照权重从大到小的顺序买入，先买优先股
        """
        weightPerTrade = weightPerTrade.sort_values(by='weight', ascending=False)
        total_weight = weightPerTrade['weight'].sum()
        # factor = self.__config.stock_position / total_weight
        # weightPerTrade['weight'] = weightPerTrade['weight'] * factor

        total_weight = weightPerTrade['weight'].sum()
        weightPerTrade['weight'] = weightPerTrade['weight'] / total_weight

        windcode_list = weightPerTrade['windcode'].tolist()
        weight_list = weightPerTrade['weight'].tolist()

        target_weight = OrderedDict()
        for inst, weight in zip(windcode_list, weight_list):
            target_weight[inst] = weight

        broker = self.getBroker()

        untradable_position = OrderedDict()
        current_amount = OrderedDict()
        total_untradeable_amount = 0
        # 此处还是依照一个先卖后买的基本顺序
        # 清仓不在目标持仓中的股票
        if len(self.getBroker().getPositions()) > 0:
            instruments = list(self.getBroker().getPositions().keys()) # 注意持仓map会变动
            for inst in instruments:
                try:
                    weight = target_weight[inst]
                    current_amount[inst] = broker.getSharesAmount(inst, bars)
                    continue
                except KeyError:
                    pass
                shares = self.getBroker().getShares(inst)
                if shares == 0:
                    continue
                # print 'sell out %s'%(inst)
                self.enterShort(bars, inst, shares, False, False)
                remaining_amount = self.getBroker().getSharesAmount(inst, bars)
                if remaining_amount > 0:
                    untradable_position[inst] = remaining_amount
                    total_untradeable_amount += remaining_amount

        total_equity = broker.get_total_equity()
        untradable_ratio = total_untradeable_amount / total_equity

        new_tradable_ratio = self.__config.stock_position - untradable_ratio # 可调仓的比例
        assert new_tradable_ratio > 0

        untradable_target_position = OrderedDict()  # 原有持仓中有，且不可交易的标的
        tradable_target_position = OrderedDict()  # 剔除上述标的后剩余的标的

        for inst, weight in target_weight.items():
            bar = self.__current_bars[inst]
            tradable = not (bar.getTradeStatus() != SuspensionType.NORMAL and self.__config.suspension_limit)

            if tradable:
                tradable_target_position[inst] = weight  # 注意，涨跌停的也划分为可交易
            else:
                untradable_target_position[inst] = weight


        total_weight = 0  # 使用可交易的票填满目标持仓，不可交易的票仅用于生成未成交信息
        for inst, weight in tradable_target_position.items():
            total_weight += weight

        check_weight = 0
        for inst, weight in tradable_target_position.items():
            tradable_target_position[inst] = weight / total_weight * new_tradable_ratio
            check_weight += weight / total_weight * new_tradable_ratio

        for inst, weight in untradable_target_position.items():
            untradable_target_position[inst] = weight / total_weight * new_tradable_ratio

        """
        先对要减仓的进行处理
        """
        for inst, weight in tradable_target_position.items():
            # 取得该股票的旧权重
            # TODO: 在持仓价值为0时的处理
            weightOld = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            if weightOld >= weight:
                self.enterLongShortWeight(bars, inst, weight, False, False)

        """
        最后对要加仓的进行处理
        """
        for inst, weight in tradable_target_position.items():
            # print inst, weight
            """
            取得该股票的旧权重
            """
            # if bars[inst].ext_status != ExtStatus.NORMAL:
            #     continue
            weightOld = self.getBroker().getSharesAmount(inst, bars) / self.getBroker().get_total_equity()
            if weightOld < weight:
                self.enterLongShortWeight(bars, inst, weight, False, False)

        """
        对不能交易的票也进行一次交易，用于生成未成交信息
        """
        for inst, weight in untradable_target_position.items():
            self.enterLongShortWeight(bars, inst, weight, False, False)

    def on_start(self):
        """
        策略开始运行时执行
        获取调仓日期
        """
        self.__buyDate = sorted(list(set(self.__weight['date'])))
        self.__trade_date = deque(self.__buyDate)
        console_log('strategy trade date is:')
        if len(self.__buyDate) > 5:
            console_log(f"{self.__buyDate[0]}, {self.__buyDate[1]}, ..., {self.__buyDate[-1]}")
        else:
            console_log(self.__buyDate)
        console_log('total trade days:', len(self.__buyDate))
        if wk_util.logger.SHOW_RUNTIME_INFO:
            from tqdm.auto import tqdm
            self.__pbar = tqdm(total=len(self.__buyDate), disable=(not self.__config.progress_bar))

        self.__target_positions = self.__weight.groupby('date')

        self.__ext_date = deque([d['trade_dt'] for d in self.__ext_status_data])

    """
    策略运行结束时执行
    """
    def on_finish(self, bars):
        if self.__pbar:
            self.__pbar.close()

    def __handle_ext_status(self, bars):
        record = self.__ext_status_data[0]
        if record['ext_status'] == ExtStatus.DUMMY_BAR_FOR_DELISTING.value:
            # 以0价格清仓退市股票
            inst = record['windcode']
            shares = self.getBroker().getShares(inst)
            self.getBroker().clean_position(bars, inst) # 直接清零，不进行交易

            # if shares > 0:
            #     # print 'sell out %s'%(inst)
            #     self.enterShort(bars, inst, shares, False, False)
        elif record['ext_status'] == ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE.value:
            # 对并购重组的情况进行调仓，根据比例变换为新的持仓
            # print('record', record)

            inst = record['windcode']
            mr_rec = self.__mr_map[inst]
            new_inst = mr_rec.new_windcode
            ratio = mr_rec.ratio * bars[inst].adj_factor  # 根据复权因子修正调仓比例
            self.getBroker().transform_shares(bars, inst, new_inst, ratio)

    def __stop_pnl(self, bars):
        """
        止盈止损操作
        """
        if not self.__config.stop_pnl:
            return

        for inst in self.getBroker().getPositions().keys():
            pnl = self.getBroker().get_pnl(bars, inst)

            if self.__config.stop_profit and pnl >= self.__config.stop_profit:
                # 止盈清仓
                self.enterLongShortWeight(bars, inst, 0, False, False)  # 清仓
            if self.__config.stop_loss and pnl <= self.__config.stop_loss:
                # 止损清仓
                self.enterLongShortWeight(bars, inst, 0, False, False)  # 清仓

    def __is_tradable(self, inst, is_buy=True, normal_only=False):
        """
        Parameters
        ----------
        normal_only: bool
            只要存在涨停或跌停就认为不可交易
        """
        bar = self.__current_bars[inst]
        tradable = not (bar.getTradeStatus() != SuspensionType.NORMAL and self.__config.suspension_limit)

        if normal_only:
            tradable = tradable and (bar.getUpDownStatus(self.__config.max_up_down_limit) == 0)
        else:
            tradable = tradable and (not (bar.getUpDownStatus(self.__config.max_up_down_limit) == 1 and is_buy))
            tradable = tradable and (not (bar.getUpDownStatus(self.__config.max_up_down_limit) == -1 and (not is_buy)))

        return tradable

    def __check_tradable(self, is_buy=True, normal_only=False):
        """
        检查现有持仓中可交易和不可交易的部分
        """
        bars = self.__current_bars
        broker: HedgeBroker = self.getBroker()
        instruments = broker.getPositions().keys()
        total_equity = broker.get_total_equity()
        tradable_ratio_map = {}
        untradable_ratio_map = {}
        untradable_ratio = 0
        position_ratio = 0
        shares_value = 0
        for inst in instruments:
            # bar = bars[inst]
            shares_value += broker.getSharesAmount(inst, bars)
            ratio = broker.getSharesAmount(inst, bars) / total_equity
            # tradable = (bar.getTradeStatus() == SuspensionType.NORMAL) and (bar.getUpDownStatus(self.__config.max_up_down_limit) == 0)
            tradable = self.__is_tradable(inst, is_buy, normal_only)
            if tradable:
                tradable_ratio_map[inst] = ratio
            else:
                untradable_ratio_map[inst] = ratio
                untradable_ratio += ratio
            position_ratio += ratio
        total_tradable_ratio = position_ratio - untradable_ratio
        new_tradable_ratio = self.__config.stock_position - untradable_ratio

        return tradable_ratio_map, untradable_ratio_map, position_ratio, untradable_ratio

    def __rebalance_stock(self, bars, force=False):
        if not force:
            return
        broker: HedgeBroker = self.getBroker()
        current_position = broker.getSharesValue() / broker.get_total_equity()

        if current_position > self.__config.stock_position:
            # 配平股票比例时需要加仓
            tradable_ratio_map, untradable_ratio_map, position_ratio, untradable_ratio = self.__check_tradable(False)
        else:
            tradable_ratio_map, untradable_ratio_map, position_ratio, untradable_ratio = self.__check_tradable(True)

        total_tradable_ratio = position_ratio - untradable_ratio
        new_tradable_ratio = self.__config.stock_position - untradable_ratio

        if new_tradable_ratio > 0:
            for inst, ratio in tradable_ratio_map.items():
                self.enterLongShortWeight(bars, inst, ratio / total_tradable_ratio * new_tradable_ratio)

    def __rebalance_future(self, bars, force=False, target_position=None):
        """
        对期货市值进行重平衡
        """
        if target_position is None:
            target_position = self.__config.stock_position * self.__config.hedge_ratio
        date_str = bars.getDateTime().strftime("%Y%m%d")
        instrument = self.__current_contract
        position = self.getBroker().getFuturePosition(instrument)
        if position is not None:
            if bars[instrument].end_date == date_str:
                # 到期换仓
                next_inst = FutureUtil.next_contract(instrument)
                self.enter_future_long_short_weight(bars, instrument, 0)
                self.enter_future_long_short_weight(bars, next_inst, -target_position)
                self.__current_contract = next_inst
            elif force:
                self.enter_future_long_short_weight(bars, instrument, -target_position)
        else:
            # 无期货仓位，建仓
            prev_contract = FutureUtil.prev_contract(self.__current_contract)
            next_contract = FutureUtil.next_contract(self.__current_contract)
            if bars[self.__current_contract].start_date <= date_str < bars[self.__current_contract].end_date:
                pass
            elif bars[prev_contract].end_data == date_str:
                pass
            elif bars[prev_contract].end_data > date_str:
                self.__current_contract = prev_contract
            elif bars[self.__current_contract].end_date <= date_str:
                self.__current_contract = next_contract
            else:
                assert False
            self.enter_future_long_short_weight(bars, self.__current_contract, -target_position)

    def __check_hedge(self, bars):
        """
        检查对冲情况
        """
        broker: HedgeBroker = self.getBroker()
        shares_value = broker.getSharesValue()
        future_value = broker.getFutureValue()
        # total_equity = broker.total_equity_pre
        # available_cash = broker.getAvailableCash()
        # frozen_cash = broker.frozen_cash

        if shares_value != 0:
            hedge_ratio = - future_value / shares_value
            max_deviation = self.__config.max_hedge_deviation
            ratio = hedge_ratio / self.__config.hedge_ratio
            force = ratio < (1 - max_deviation) or ratio > (1 + max_deviation)
            # print('[rebalance]', force)s
            force = force or (broker.deposit_ratio > self.__config.deposit_cash_ratio)
            # print('rebalance', force, bars.getDateTime().strftime("%Y%m%d"),
            #       hedge_ratio, broker.deposit_ratio, max_exposure, self.__config.deposit_cash_ratio)
            # TODO: 股票价值变动可能导致重平衡时保期货保证金不足
            self.__rebalance_stock(bars, force)
            target_position = broker.getSharesValue() / broker.get_total_equity() * self.__config.hedge_ratio
            # target_position = target_position * self.__config.hedge_ratio
            self.__rebalance_future(bars, force, target_position)
            try:
                assert broker.deposit_ratio < 1
            except Exception as e:
                shares_value = broker.getSharesValue()
                future_value = broker.getFutureValue()
                available_cash = broker.getAvailableCash()
                total_equity = broker.get_total_equity()
                frozen_cash = broker.frozen_cash
                print("[shares value]\t", shares_value,
                      "\n[future value]\t", future_value,
                      "\n[cash]", broker.getCash(),
                      "\n[available cash]\t", available_cash,
                      "\n[total equity]\t", total_equity,
                      "\n[frozen cash]\t", frozen_cash)
                raise e

    def __update_hedge(self, bars):
        broker: HedgeBroker = self.getBroker()
        target_position = broker.getSharesValue() / broker.get_total_equity() * self.__config.hedge_ratio
        try:
            self.__rebalance_future(bars, force=True, target_position=target_position)
        except Exception as e:
            print('[target position]', target_position)
            raise e

    def on_bars(self, bars):
        """
        每天的数据流到来时触发一次
        """
        date_str = bars.getDateTime().strftime("%Y%m%d")  # 注意时间格式
        self.__current_bars = bars

        while len(self.__ext_date) > 0 and date_str == self.__ext_date[0]:
            self.__handle_ext_status(bars)
            self.__ext_date.popleft()
            self.__ext_status_data.popleft()

        while len(self.__trade_date) > 0 and date_str > self.__trade_date[0]:  # 处理非交易日调仓的情况
            # TODO: 日志中增加警告
            self.__trade_date.popleft()
            if self.__pbar:
                self.__pbar.update(1)
        if len(self.__trade_date) > 0 and date_str == self.__trade_date[0]:
            self.__trade_date.popleft()
            # print(date_str)
            broker: HedgeBroker = self.getBroker()

            self.__change_position(bars)
            self.__update_hedge(bars)
            if self.__pbar:
                self.__pbar.update(1)
        else:
            self.__stop_pnl(bars)
            self.__check_hedge(bars)

    @classmethod
    def prepare_feed(cls, begin_date, end_date, instruments, config):
        ds = DataSource()
        mr_map = ds.get_mr_data()

        ext_instrument = []
        for inst in instruments:
            try:
                mr_record = mr_map[inst]
                ext_instrument.append(mr_record.new_windcode)
            except KeyError:
                continue
        instruments = instruments + ext_instrument

        stock_data = ds.get_daily(begin_date, end_date)
        data = pd.DataFrame({"windcode": instruments}).merge(stock_data, on="windcode")
        ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")

        feed = StockFutureFeed()
        feed.add_stock_bars(data, progress_bar=config.progress_bar)

        future_data = ds.get_spif_data(config.code, begin_date, end_date)
        feed.add_future_bars(future_data, progress_bar=config.progress_bar)

        return feed, ext_status_df, mr_map

    @classmethod
    def strategy_name(cls):
        return "HedgeStrategy"


class HedgeStrategy:
    """
    根据权重列表定期调仓的回测策略
    """
    def __init__(self, weight, begin_date, end_date=None, is_tag=False, config=HedgeStrategyConfiguration()):
        """
        Parameters
        ----------
        weight: pd.DataFrame or str
            调仓权重列表DataFrame或权重文件路径

        begin_date: str
            yyyymmdd 格式的日期字符串

        end_date: str
            yyyymmdd 格式的日期字符串

        config: StrategyConfiguration
            策略配置类
        """
        console_log('platform version:', __version__)
        console_log('strategy: Hedge Strategy')
        if is_tag:
            data = wk_db.read_weight(weight)
            self.__weight_df = data.sort_values(['date', 'windcode'])
            self.__sign = None
        elif isinstance(weight, pd.DataFrame):
            self.__weight_df = weight.sort_values(['date', 'windcode'])
            self.__sign = None
        else:
            assert isinstance(weight, str) or isinstance(weight, pathlib.Path)
            self.__weight_df = pd.read_csv(weight, encoding="gbk").sort_values(['date', 'windcode'])
            self.__sign = md5(weight)

        check_weight_df(self.__weight_df)

        self.__begin_date = begin_date
        self.__end_date = end_date
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result = None
        self.__config = config
        console_log("backtest range:", self.__begin_date, self.__end_date)

    def __prepare_feed(self):
        ds = DataSource()
        self.__mr_map = ds.get_mr_data()
        begin_date, end_date = self.__begin_date, self.__end_date
        weight_df = self.__weight_df
        # weight_df['date'] = pd.to_datetime(weight_df['date']).map(lambda x: x.strftime('%Y%m%d'))
        weight_df['date'] = pd.to_datetime(weight_df['date'], format='%Y%m%d').apply(lambda x: x.strftime('%Y%m%d'))
        idx = (weight_df['date'] >= begin_date) & (weight_df['date'] <= end_date)
        weight_df = weight_df[idx]
        instruments = weight_df['windcode'].unique()

        ext_instrument = []
        for inst in instruments.tolist():
            try:
                mr_record = self.__mr_map[inst]
                ext_instrument.append(mr_record.new_windcode)
            except KeyError:
                continue
        instruments = instruments.tolist() + ext_instrument

        stock_data = ds.get_daily(begin_date, end_date)
        data = pd.DataFrame({"windcode": instruments}).merge(stock_data, on="windcode")
        self.__ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")

        self.__feed = StockFutureFeed()
        self.__feed.add_stock_bars(data)

        future_data = ds.get_spif_data(self.__config.code, begin_date, end_date)
        self.__feed.add_future_bars(future_data)
        self.__feed.prefetch(progress_bar=self.__config.progress_bar)

    def run(self):
        self.__prepare_feed()
        begin_date, end_date = self.__begin_date, self.__end_date
        datVal = self.__weight_df

        datVal['date'] = pd.to_datetime(datVal['date'], format='%Y%m%d').map(lambda x: x.strftime('%Y%m%d'))
        datVal = datVal[(datVal['date'] >= begin_date) & (datVal['date'] <= end_date)]

        """
        设定起始日期为首行日期
        """
        tmp = datVal['date']
        begin_date = tmp.iloc[0]
        weight_strategy = HedgeStrategyBase(self.__feed, datVal, begin_date, end_date, self.__config,
                                            ext_status_data=self.__ext_status_df.to_dict(orient="records"),
                                            mr_map=self.__mr_map, sign=self.__sign)

        output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
        output.pre_process()
        weight_strategy.run()
        output.bench_process()
        output.post_process()
        self.__result = output.result

    @classmethod
    def strategy_class(cls):
        return HedgeStrategyBase

    @property
    def result(self) -> BackTestResult:
        return self.__result


class BatchHedgeStrategy:
    """
    批量执行权重回测的策略
    """
    def __init__(self, files, begin_date, end_date=None, config=HedgeStrategyConfiguration(), max_process=1):
        console_log('platform version:', __version__)
        self.__result_set = [None for f in files]
        self.__files = []
        for file in files:
            if isinstance(file, pathlib.Path):
                self.__files.append(file)
            else:
                self.__files.append(pathlib.Path(file))
        self.__weights = {}
        self.__signs = {}
        self.__begin_date = begin_date
        self.__end_date = end_date
        if self.__end_date is None:
            self.__end_date = (datetime.datetime.now() + timedelta(days=1)).strftime("%Y%m%d")
        self.__result_set = BackTestResultSet()
        self.__config = config
        console_log("backtest range:", self.__begin_date, self.__end_date)
        self.__prepare_weight_df()

    def __prepare_weight_df(self):
        for file in self.__files:
            k = file.stem
            self.__weights[k] = pd.read_csv(file, encoding='gbk').sort_values(['date', 'windcode'])
            self.__signs[k] = md5(file)

    def __prepare_feed(self):
        ds = DataSource()
        self.__mr_map = ds.get_mr_data()
        begin_date, end_date = self.__begin_date, self.__end_date

        data = ds.get_daily(begin_date, end_date)
        instruments = []
        for k, weight_df in self.__weights.items():
            instruments += weight_df['windcode'].unique().tolist()
        instruments = list(set(instruments))

        ext_instrument = []
        for inst in instruments:
            try:
                mr_record = self.__mr_map[inst]
                ext_instrument.append(mr_record.new_windcode)
            except KeyError:
                continue

        instruments = instruments + ext_instrument
        # print(instruments.index('603195.SH'))
        data = pd.DataFrame({"windcode": instruments}).merge(data, on="windcode")
        self.__ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
        self.__feed = None # prepare_feed(data, self.__begin_date, self.__end_date, self.__config.progress_bar)

    def run(self):
        self.__prepare_feed()
        begin_date, end_date = self.__begin_date, self.__end_date
        for name, weight_df in self.__weights.items():
            datVal = weight_df

            datVal['date'] = pd.to_datetime(datVal['date'], format='%Y%m%d')
            datVal['date'] = [datetime.datetime.strftime(x, '%Y%m%d') for x in datVal['date']]
            datVal = datVal[(datVal['date'] >= begin_date) & (datVal['date'] <= end_date)]

            """
            设定起始日期为首行日期
            """
            tmp = datVal['date']
            begin_date = tmp.iloc[0]
            weight_strategy = HedgeStrategyBase(self.__feed, datVal, begin_date, end_date, self.__config,
                                                 ext_status_data=self.__ext_status_df.to_dict(orient="records"),
                                                 mr_map=self.__mr_map, sign=self.__signs[name])

            output = strategyOutput.StrategyOutput(weight_strategy, begin_date, end_date, config=self.__config)
            output.pre_process()
            weight_strategy.run()
            output.bench_process()
            output.post_process()
            self.__result_set[name] = output.result
            self.__feed.reset()

    @property
    def result_set(self) -> BackTestResultSet:
        return self.__result_set








