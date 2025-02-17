# from wk_data.constants import ExtStatus
# from wk_platform.contrib.util import PreprocessorSeq
# from wk_platform.feed.parser import StockDataRowParser
#
#
# def add_bar_a_share_market(feed, begin_date, end_date):
#     # if dataset == DatasetType.A_SHARE_MARKET:
#     feed.add_bars('a_share_market', StockDataRowParser, begin_date=begin_date, end_date=end_date,
#                       preprocessor=PreprocessorSeq(
#                           partial(filter_market_data, instruments=instruments),
#                           align_func
#                       ))
#     data = feed.get_processed_data('a_share_market')
#     ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
#     ext_status_df = ext_status_df[
#         ext_status_df['ext_status'] != ExtStatus.UNTRADABLE_FILLED_BAR.value
#         ].sort_values(by="trade_dt")  # 填充值不需要特殊处理
#
#     if not data.empty:
#         end_date = data['trade_dt'].max()  # 强制对齐



    # elif dataset == DatasetType.SYNTH_INDEX_ETF:
    #     feed.add_bars('index_market', SyntheticIndexETFRowParser,
    #                   bars_name='synth_index_etf', begin_date=begin_date, end_date=end_date,
    #                   preprocessor=PreprocessorSeq(
    #                       add_normal_ext_status,
    #                       align_func
    #                   ))
    # elif dataset == DatasetType.AME_SYNTH_INDEX_ETF:
    #     feed.add_bars('ame_index', SyntheticIndexETFRowParser,
    #                   bars_name='ame_synth_index_etf', begin_date=begin_date, end_date=end_date,
    #                   preprocessor=PreprocessorSeq(
    #                       add_normal_ext_status,
    #                       align_func
    #                   ))
    #
    # elif dataset == DatasetType.ETF_FUND_SUBSET_1:
    #     feed.add_bars('etf_fund_subset_1', FundDataRowParser, begin_date=begin_date, end_date=end_date,
    #                   preprocessor=align_func)
    #     data = feed.get_processed_data('etf_fund_subset_1')
    #     ext_status_df = data[data['ext_status'] != ExtStatus.NORMAL.value].sort_values(by="trade_dt")
    #     # assert config.price_type == PriceType.CLOSE, \
    #     #     f'`price_type` must be `close` when using dataset `{dataset.name.lower()}`'
    # else:
    #     raise ValueError(f"Unsupported dataset `{dataset}`")