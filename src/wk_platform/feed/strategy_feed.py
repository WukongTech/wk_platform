from functools import partial

import pandas as pd

from wk_data.constants import ExtStatus
from wk_platform.config.enums import DatasetType, PriceType

import wk_data
from wk_platform.contrib.util import PreprocessorSeq
from wk_platform.feed.mixed_feed import MixedFeed
from wk_platform.feed.parser import StockDataRowParser, SyntheticIndexETFRowParser, IndexETFRowParser, \
    FundDataRowParser, FundNavDataRowParser, FutureDataRowParser, PositionDummyRowParser, IndexDataRowParser
from wk_platform.util.data import align_calendar, filter_market_data, add_normal_ext_status
from wk_util.logger import console_log

DATASET_FEED_MAPPING = {}


def register_feed(name: DatasetType):
    def decorator(func):
        assert name not in DATASET_FEED_MAPPING.keys()
        DATASET_FEED_MAPPING[name] = func
        return DATASET_FEED_MAPPING[name]

    return decorator



def prepare_feed_a_share_market_common(dataset, feed, config, begin_date, end_date, align_func, *, instruments=None):
    feed.add_bars(dataset, StockDataRowParser, begin_date=begin_date, end_date=end_date,
                  preprocessor=PreprocessorSeq(
                      partial(filter_market_data, instruments=instruments),
                      align_func
                  ))
    data = feed.get_processed_data(dataset)
    ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
    ext_status_df = ext_status_df[
        ext_status_df['ext_status'] != ExtStatus.UNTRADABLE_FILLED_BAR.value
        ].sort_values(by="trade_dt")  # 填充值不需要特殊处理
    end_date = data['trade_dt'].max()
    return end_date, ext_status_df


@register_feed(DatasetType.A_SHARE_MARKET)
def prepare_feed_a_share_market(feed, config, begin_date, end_date, align_func, *, instruments=None):
    return prepare_feed_a_share_market_common('a_share_market', feed, config, begin_date, end_date, align_func, instruments=instruments)


@register_feed(DatasetType.SYNTH_INDEX_ETF)
def prepare_feed_synth_index_etf(feed, config, begin_date, end_date, align_func):
    dt_list = []
    m1 = feed.add_bars('index_market', SyntheticIndexETFRowParser,
                  bars_name='synth_index_etf', begin_date=begin_date, end_date=end_date,
                  preprocessor=PreprocessorSeq(
                      add_normal_ext_status,
                      align_func
                  ))
    if m1 is not None:
        dt_list.append(m1)
    m2 = feed.add_bars('ame_index_market', SyntheticIndexETFRowParser,
                  bars_name='synth_ame_index_etf', begin_date=begin_date, end_date=end_date,
                  preprocessor=PreprocessorSeq(
                      add_normal_ext_status,
                      align_func
                  ))

    if m2 is not None:
        dt_list.append(m2)
    m3 = feed.add_bars('commodity_index', SyntheticIndexETFRowParser,
                  bars_name='synth_commodity_index_etf', begin_date=begin_date, end_date=end_date,
                  preprocessor=PreprocessorSeq(
                      add_normal_ext_status,
                      align_func
                  ))

    if m3 is not None:
        dt_list.append(m3)

    if len(dt_list) == 0:
        max_time = None
    else:
        max_time = min(dt_list)

    return max_time, None


@register_feed(DatasetType.A_SHARE_ETF)
def prepare_feed_a_share_etf(feed, config, begin_date, end_date, align_func):
    max_time = feed.add_bars('a_share_etf', IndexETFRowParser,
                             bars_name='a_share_etf', begin_date=begin_date, end_date=end_date,
                             preprocessor=PreprocessorSeq(
                                 add_normal_ext_status,
                                 align_func
                             ))
    return max_time, None


@register_feed(DatasetType.LOCAL_INDEX_ETF)
def prepare_feed_local_index_etf(feed, config, begin_date, end_date, align_func):
    max_time = feed.add_bars('local_index_20240417', SyntheticIndexETFRowParser,
                             bars_name='local_synth_index_etf', begin_date=begin_date, end_date=end_date,
                             preprocessor=PreprocessorSeq(
                                 add_normal_ext_status,
                                 align_func
                             ))
    return max_time, None


@register_feed(DatasetType.SPECIAL_INDEX)
def prepare_feed_special_index(feed, config, begin_date, end_date, align_func):
    assert config.price_type == PriceType.CLOSE, \
        f'`price_type` must be `close` when using dataset `{DatasetType.SPECIAL_INDEX}`'
    max_time = feed.add_bars('special_index', FundNavDataRowParser, begin_date=begin_date, end_date=end_date,
                  preprocessor=align_func)
    return max_time, None


@register_feed(DatasetType.INDEX_FUTURE)
def prepare_feed_index_future(feed, config, begin_date, end_date, align_func):
    max_time = feed.add_bars('spif_data', FutureDataRowParser,
                             bars_name='index_future', begin_date=begin_date, end_date=end_date,
                             preprocessor=PreprocessorSeq(
                                 add_normal_ext_status,
                                 align_func
                             ))
    return max_time, None


@register_feed(DatasetType.ETF_FUND_SUBSET_1)
def prepare_feed_etf_func_subset_1(feed, config, begin_date, end_date, align_func):
    max_time = feed.add_bars('etf_fund_subset_1', FundDataRowParser, begin_date=begin_date, end_date=end_date,
                  preprocessor=align_func)
    data = feed.get_processed_data('etf_fund_subset_1')
    ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
    return max_time, ext_status_df


class FeedRegistry:
    def __init__(self, begin_date, end_date, config):
        self.begin_date = begin_date
        self.end_date = end_date
        self.config = config
        self.context = {}

    def register(self, dataset_name, **kwargs):
        key = DatasetType[dataset_name.upper()]
        self.context[key] = kwargs

    def build_feed(self):
        console_log("preparing feed...")

        local_calendar = wk_data.get('trade_calendar', begin_date=self.begin_date, end_date=self.end_date,
                                     calendar=self.config.calendar)
        align_func = partial(align_calendar, calendar=local_calendar)
        feed = MixedFeed()
        ext_status_df = pd.DataFrame()
        max_dt_list = []
        for dataset in self.config.datasets:
            dataset = DatasetType[dataset.upper()]
            kwargs = self.context[dataset]
            prepare_func = DATASET_FEED_MAPPING[dataset]
            max_dt, ret_ext_df = prepare_func(feed, self.config, self.begin_date, self.end_date, align_func, **kwargs)
            if max_dt is not None:
                max_dt_list.append(max_dt)
            if ret_ext_df is not None:
                assert ext_status_df.empty
                ext_status_df = ret_ext_df

        feed.add_bars('dummy_data', PositionDummyRowParser, begin_date=self.begin_date, end_date=self.end_date,
                      preprocessor=align_func)

        feed.add_bars('index_market', IndexDataRowParser, begin_date=self.begin_date, end_date=self.end_date,
                      preprocessor=align_func)

        feed.prefetch(self.config.progress_bar)

        return feed, ext_status_df

