from functools import lru_cache

import pandas as pd
import statsmodels.api as sm

from wk_data.data_source import RiskFactorDataSource
from wk_data.data_source import DataSource
from wk_util.logger import console_log
from wk_data.constants import STType, ExtStatus, BENCH_MAP

FACTOR_LIST = [
    "residual_volatility",
    "momentum",
    "earnings_yield",
    "comovement",
    "growth",
    "liquidity",
    "size",
    "leverage",
    "book_to_price",
    "beta",
    "non_linear_size",
]

SECTOR_LIST = [
    '电气设备', '通信', '休闲服务', '房地产', '电子', '医药生物', '非银金融', '计算机', '纺织服装', '公用事业', '机械设备',
    '轻工制造', '汽车', '商业贸易', '化工', '采掘', '食品饮料', '建筑装饰', '国防军工', '农林牧渔', '建筑材料', '传媒',
    '有色金属', '银行', '综合', '家用电器', '交通运输', '钢铁',
]


def portfolio_factor_exposure(weight_df: pd.DataFrame, trade_dt):
    console_log('loading factor exposure...')
    rdf = RiskFactorDataSource()
    factor_exposure = rdf.get_factor_exposure(trade_dt)

    factor_df = weight_df.merge(factor_exposure, on='windcode')

    total_weight = weight_df['weight'].sum()
    weight_df['weight'] = weight_df['weight'] / total_weight

    portfolio_exposure = {}
    for factor in FACTOR_LIST:
        portfolio_exposure[factor] = (weight_df['weight'] * factor_df[factor]).sum()
    return portfolio_exposure


def filter_stock_data(price_data, *, use_adjusted=True, exclude_st=True):
    price_data = price_data[price_data['ext_status'] == ExtStatus.NORMAL.value]

    if exclude_st:
        price_data = price_data[price_data['st'] == '']

    if use_adjusted:
        price_data['open'] = price_data['open'] * price_data['adj_factor']
        price_data['close'] = price_data['close'] * price_data['adj_factor']
    return price_data



def calc_stock_return(begin_date, end_date, *, use_adjusted=True, exclude_st=True):
    """
    计算给定时间区间的个股收益率
    """
    ds = DataSource()
    price_data = ds.get_daily(begin_date, end_date)

    data_list = [
        price_data[price_data['trade_dt'] == begin_date],
        price_data[price_data['trade_dt'] == end_date]
    ]
    price_data = pd.concat(data_list)

    price_data = filter_stock_data(price_data, use_adjusted=use_adjusted, exclude_st=exclude_st)

    mr_map = ds.get_mr_data()

    def group_func(df):
        if df.shape[0] == 1:
            windcode = df.iloc[0]['windcode']
            try:
                mr_record = mr_map[windcode]
                new_windcode = mr_record.new_windcode
                new_entry = price_data[price_data['windcode'] == new_windcode]
                new_entry['open'] = new_entry['open'] * mr_record.ratio
                new_entry['close'] = new_entry['close'] * mr_record.ratio
                new_entry['windcode'] = windcode
                df = pd.concat([df, new_entry])
            except KeyError:
                # 没有发生吸收合并，视为退市情形
                return pd.DataFrame()
        pct_return = df.reset_index()[['open', 'close']].pct_change().dropna()
        return pct_return

    price_data = price_data.groupby('windcode').apply(group_func)
    price_data = price_data.reset_index().drop('level_1', axis=1)
    return price_data


def prepare_risk_factor(begin_date):
    """
    构造因子数据DataFrame，主要将行业列转换为哑变量，并补充市值
    """
    rdf = RiskFactorDataSource()
    market_value = rdf.get_market_value(begin_date)

    factor_exposure = rdf.get_factor_exposure(begin_date)

    sector_data = factor_exposure[['date', 'windcode', 'sector']].copy()
    sector_data['__value'] = 1
    pivot_data = sector_data.pivot_table(index=['windcode', 'date'], columns=['sector'], fill_value=0)
    pivot_data.columns = pivot_data.columns.droplevel()
    pivot_data.columns.name = None
    pivot_data.reset_index()
    full_data = factor_exposure.merge(pivot_data, on=['date', 'windcode'])

    full_data = full_data.merge(market_value, on="windcode")
    return full_data


@lru_cache(maxsize=4)
def calc_factor_return(begin_date, end_date):

    ds = DataSource()
    trade_calender = ds.get_trade_calendar(begin_date, end_date)
    if begin_date != trade_calender[0] or end_date != trade_calender[-1]:
        begin_date = trade_calender[0]
        end_date = trade_calender[-1]
    console_log(f'calculate factor return between {begin_date} {end_date}')
    stock_return = calc_stock_return(begin_date, end_date)
    risk_factor = prepare_risk_factor(begin_date)
    full_data = stock_return.merge(risk_factor, on="windcode")

    sector_mv = pd.DataFrame(risk_factor.groupby('sector')['market_value'].sum()).to_dict()['market_value']

    base_sector = '综合'
    sector_mv_comp = sector_mv[base_sector]
    partial_sector_list = [s for s in SECTOR_LIST if s != base_sector]
    for sector in partial_sector_list:
        full_data[sector] = full_data[sector] - sector_mv[sector] / sector_mv[base_sector] * full_data[base_sector]

    exog_list = FACTOR_LIST + partial_sector_list + ['fm']
    full_data['fm'] = 1
    console_log('running regression...')
    wls_model = sm.WLS(full_data['close'], full_data[exog_list], weights=full_data['market_value'])
    result = wls_model.fit()
    factor_ret = result.params.to_dict()
    base_sector_ret = 0
    for sector in partial_sector_list:
        base_sector_ret += sector_mv[sector] * factor_ret[sector]
    factor_ret[base_sector] = -base_sector_ret / sector_mv[base_sector]

    return factor_ret


def factor_return(begin_date, end_date, *, with_sector=True):
    f_ret = calc_factor_return(begin_date, end_date)
    f_ret = pd.Series(f_ret).reset_index().rename(columns={0: '因子收益'})
    if not with_sector:
        f_ret = pd.DataFrame({'index': FACTOR_LIST}).merge(f_ret, on='index', how='left')
    f_ret.rename(columns={'index': '因子'}, inplace=True)
    return f_ret


def portfolio_factor_return(weight_df, begin_date, end_date):
    f_e = portfolio_factor_exposure(weight_df, begin_date)
    f_e = pd.Series(f_e).reset_index().rename(columns={0: '因子暴露'})
    f_ret = calc_factor_return(begin_date, end_date)
    f_ret = pd.Series(f_ret).reset_index().rename(columns={0: '因子收益'})

    data = f_e.merge(f_ret, on='index', how='left')
    data['收益贡献'] = data['因子收益'] * data['因子暴露']
    data.rename(columns={'index': '因子'}, inplace=True)
    return data


def bench_factor_return(windcode, begin_date, end_date):
    """
    计算基准组合的因子收益率
    """
    # assert windcode in ('000300.SH', )
    rdf = RiskFactorDataSource()
    bench_weight_df = rdf.get_index_stock_weight(windcode, begin_date)
    bench_fr = portfolio_factor_return(bench_weight_df, begin_date, end_date)
    return bench_fr


def relative_factor_return(weight_df, bench, begin_date, end_date):
    p_fr = portfolio_factor_return(weight_df, begin_date, end_date)
    b_fr = bench_factor_return(bench, begin_date, end_date)
    bench_name = BENCH_MAP[bench]
    # b_fr.rename(columns={
    #     '因子暴露': bench_name + '因子暴露',
    #     '因子收益': bench_name + '因子收益',
    #     '收益贡献': bench_name + '收益贡献',
    # })
    data = p_fr.merge(b_fr, on='因子')[['因子', '因子收益_x', '因子暴露_x', '收益贡献_x', '因子暴露_y', '收益贡献_y']]
    data['相对因子暴露'] = data['因子暴露_x'] - data['因子暴露_y']
    data['相对收益贡献'] = data['收益贡献_x'] - data['收益贡献_y']
    data.rename(columns={
        '因子暴露_x': '因子暴露',
        '收益贡献_x': '收益贡献',
        '因子收益_x': '因子收益',
        '因子暴露_y': bench_name + '因子暴露',
        '收益贡献_y': bench_name + '收益贡献',
    }, inplace=True)
    return data





