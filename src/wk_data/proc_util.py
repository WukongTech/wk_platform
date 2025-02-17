import os
import pathlib

import numpy as np
import pandas as pd
from wk_data.constants import ExtStatus
from wk_util.logger import console_log


def proc_na(data_part):
    data_part = data_part.sort_values(by='trade_dt')
    # 此处-100的设定需要再次确认
    # data_part[data_part == -100] = np.nan
    # data_part = data_part.fillna(method='ffill')
    data_part = data_part.ffill()
    return data_part


def generate_dummy_data(row, begin_date, end_date, trade_calendar: [str]):
    """
    通过复制row生成begin_date到end_date闭区间内所有交易日的数据
    """
    begin_idx = trade_calendar.index(begin_date)
    end_idx = trade_calendar.index(end_date) + 1

    # 复制的同时保留类型信息
    row = row.reset_index(drop=True)
    data = row.loc[[0] * (end_idx - begin_idx)]

    data.columns = row.columns
    data['trade_dt'] = trade_calendar[begin_idx:end_idx]
    data['volume'] = 0
    data['amount'] = 0
    data['suspension'] = 100001000  # 填充数据缺失停牌的代码
    return data


def mark_delisting(data: pd.DataFrame, delist_date, trade_calendar=None):
    last_row = data.tail(1).to_dict(orient='records')[0]

    if last_row['ext_status'] == ExtStatus.DUMMY_BAR_FOR_DELISTING.value:
        # 已经添加过退市日的虚拟行情
        return data
    last_row['trade_dt'] = delist_date
    last_row['open'] = 0
    last_row['close'] = 0
    last_row['high'] = 0
    last_row['low'] = 0
    last_row['volume'] = np.inf
    last_row['ext_status'] = ExtStatus.DUMMY_BAR_FOR_DELISTING.value

    last_row = pd.DataFrame(last_row, index=[0])

    data = pd.concat([data, last_row])
    return data


def mark_merger_and_reorganization(data: pd.DataFrame, delist_date, current_date, trade_calendar=None, mr_map=None):
    last_row = data.tail(1).to_dict(orient='records')[0]
    mr_record = mr_map[last_row['windcode']]
    if last_row['trade_dt'][0] == current_date:
        # 当前年度没有并购重组，并且没有数据缺失
        return data

    if delist_date < trade_calendar[0]:
        begin_date = trade_calendar[0]
    else:
        begin_date = delist_date
    # print(' mr_record.change_dt >= trade_calendar[-1]:', type(mr_record.change_dt), mr_record.change_dt,
    #       type(trade_calendar[-1]), trade_calendar[-1])
    if mr_record.change_dt >= trade_calendar[-1]:
        # 有并购重组，停复牌时间出现了跨年 （另有复牌时间恰好在新年第一个交易日
        dummy_data = generate_dummy_data(data.tail(1), begin_date, trade_calendar[-1], trade_calendar)
        data = pd.concat([data, dummy_data]).reset_index(drop=True)
    else:
        if mr_record.change_dt < trade_calendar[0]:
            # 上一年度内的并购，本期不需要考虑
            idx = data.shape[0] - 1
            assert data.loc[idx, ('ext_status',)] == ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE.value
            return data
        idx = trade_calendar.index(mr_record.change_dt) + 1
        mark_date = trade_calendar[idx]
        dummy_data = generate_dummy_data(data.tail(1), begin_date, mark_date, trade_calendar)
        data = pd.concat([data, dummy_data]).reset_index(drop=True)
        idx = data.shape[0] - 1
        data.loc[idx, ('ext_status',)] = ExtStatus.DUMMY_BAR_FOR_CHANGING_WINDCODE.value

    return data


def mark_delisting_merger_and_reorganization(data: pd.DataFrame, current_date, trade_calendar=None, mr_map=None):
    """
    对退市、并购重组等情况做出标记
    1. 退市：新增退市日的行情，假定当天的价格为0，可交易量无穷大，模拟0元清仓
    2. 并购重组：标记新的代码和转换参数
    """
    # 将ext_status全部标记为正常
    try:
        status = data['ext_status']  # 对于增量更新原数据中已经包含此列
    except KeyError:
        data['ext_status'] = ExtStatus.NORMAL.value

    last_row = data.tail(1).to_dict(orient='records')[0]
    # 忽略已经标记过的数据
    if last_row['ext_status'] != ExtStatus.NORMAL.value:
        return data

    # 检查是否存在delist_date
    try:
        # 注意部分股票delist_date字段同时存在退市日期和空值
        delist_date = data['delist_date'].dropna().unique()[0]
    except IndexError:
        # delist_date全部为空值，可能的情况
        # 1. 正常交易
        # 2. 吸收合并，数据未更新
        # 3. 退市，数据未更新

        # 如果为正常交易
        if last_row['trade_dt'] == current_date:
            return data
        # 如果是吸收合并未更新或剩余情况
        idx = trade_calendar.index(last_row['trade_dt']) + 1
        delist_date = trade_calendar[idx]

    if current_date < delist_date:
        # 退市/并购重组不在当期
        return data

    if mr_map is None:
        mr_map = {}

    try:
        mr_record = mr_map[last_row['windcode']]  # 如果在并购/重组记录表中
        return mark_merger_and_reorganization(data, delist_date, current_date, trade_calendar, mr_map)
    except KeyError:
        # 不在并购重组表中
        return mark_delisting(data, delist_date, trade_calendar)


def pre_calculate(data: pd.DataFrame, window=20):
    """
    预计算字段
    """
    ma = data['amount'].rolling(window).mean()
    idx = ma.isna()
    assert idx.sum() == window - 1 or idx.sum() == len(idx)
    # 没有 ma20 的前19个数据直接用原始数据填充
    ma[idx] = data['amount'][idx]
    data['amount_ma'] = ma
    return data


def group_processor(data, current_date, trade_calendar=None, mr_map=None):
    # cs = len(data.columns)
    data = proc_na(data)
    # assert len(data.columns) == cs
    begin_date = data['trade_dt'].min()

    data = mark_delisting_merger_and_reorganization(data, current_date, trade_calendar, mr_map)
    # assert len(data.columns) == cs + 1

    data = pre_calculate(data)

    # 填充数据的生成根据trade_calendar确定，并会根据begin_date作裁剪
    idx = (data['trade_dt'] >= begin_date) & (data['trade_dt'] <= current_date)
    data = data[idx]
    return data


def fillna_with_previous_data(data, previous_data, trade_calendar=None, mr_map=None, using_parallel=False):
    """
    考虑前值的缺失值填充
    """
    begin_date = data['trade_dt'].min()
    current_date = data['trade_dt'].max()  # 用于在动态更新时确定是否保留delist_date对应的虚拟数据

    full_data: pd.DataFrame = pd.concat([previous_data[previous_data['trade_dt'] < begin_date], data]).reset_index(drop=True)

    def group_processor_curried(data_part):
        return group_processor(data_part, current_date, trade_calendar, mr_map)
    if using_parallel:
        full_data = full_data.groupby("windcode", group_keys=True).parallel_apply(group_processor_curried).reset_index(drop=True)
    else:
        full_data = full_data.groupby("windcode", group_keys=True).apply(group_processor_curried).reset_index(drop=True)
    selected_data = full_data[
        ['trade_dt', 'windcode', 'pre_close', 'open', 'high', 'low', 'close', 'industry_name', 'volume', 'st', 'suspension',
         'sec_name', 'max_up_down', 'list_date', 'amount', 'adj_factor', 'delist_date', 'ext_status', 'amount_ma']]
    idx = selected_data['trade_dt'] >= begin_date
    selected_data = selected_data[idx]
    return selected_data


def process_price_data_procedure(full_data, previous_data=None, trade_calendar=None, mr_map=None, using_parallel=True):
    full_data = rename_columns(full_data)
    full_data = process_price_data(full_data, previous_data, trade_calendar, mr_map, using_parallel)
    return full_data


def rename_columns(data):
    mapping = {
        'trade_dt': 'trade_dt',
        's_info_windcode': 'windcode',
        's_info_name': 'sec_name',
        's_info_listdate': 'list_date',
        's_info_delistdate': 'delist_date',
        's_info_st': 'st',
        's_info_citics1_name': 'industry_name',
        's_info_suspension': 'suspension',
        's_info_limit': 'max_up_down',
        's_dq_preclose': 'pre_close',
        's_dq_open': 'open',
        's_dq_close': 'close',
        's_dq_low': 'low',
        's_dq_high': 'high',
        's_dq_volume': 'volume',
        's_dq_amount': 'amount',
        's_dq_adjfactor': 'adj_factor'
    }
    data.rename(columns=mapping, inplace=True)
    return data


def process_price_data(data, previous_data=None, trade_calendar=None, mr_map=None, using_parallel=False):
    """
    对从数据库中获取的数据进行预处理
    """
    # print("input", data.dtypes)
    current_date = data['trade_dt'].max()  # 用于在动态更新时确定是否保留delist_date对应的虚拟数据
    data['st'] = data['st'].fillna('')
    data['suspension'] = data['suspension'].fillna(0).astype(int)

    # 检验此处理的合理性
    data[data == -100] = np.nan
    fill_0_list = ['max_up_down', 'pre_close', 'open', 'close', 'low', 'high', 'volume', 'amount', 'adj_factor']
    data[fill_0_list] = data[fill_0_list].fillna(0)
    groups = data.groupby("windcode").groups
    console_log("total instrument count", len(groups.keys()))

    # 数据表中存在负数值的情况，按照windcode分组，按照trade_dt排序，负数设为Null，之后按照前值fillna
    # 该段代码执行速度较慢
    console_log("processing data")

    cs = len(data.columns)
    def group_processor_curried(data_part):
        data = group_processor(data_part, current_date, trade_calendar, mr_map)
        return data

    if using_parallel:
        processed_data = data.groupby("windcode", group_keys=True).parallel_apply(group_processor_curried)
        processed_data = processed_data.reset_index(drop=True)
    else:
        processed_data = data.groupby("windcode", group_keys=True).apply(group_processor_curried)
        processed_data = processed_data.reset_index(drop=True)

    """
    筛选实际用的列，保存成HDF5和CSV.
    该中间文件用于回测输入数据，列顺序需要严格对齐
    """
    selected_data = processed_data[
        ['trade_dt', 'windcode', 'pre_close', 'open', 'high', 'low', 'close', 'industry_name', 'volume', 'st', 'suspension',
         'sec_name', 'max_up_down', 'list_date', 'amount', 'adj_factor', 'delist_date', 'ext_status', 'amount_ma']]

    if previous_data is not None:
        # process with previous data
        selected_data = fillna_with_previous_data(selected_data, previous_data,
                                                  trade_calendar=trade_calendar,
                                                  mr_map=mr_map,
                                                  using_parallel=using_parallel)

    selected_data = selected_data.sort_values(by=['windcode', 'trade_dt'], ascending=True)
    return selected_data


def process_index_data(full_data):
    mapping = {
        'trade_dt': 'trade_dt', 's_info_windcode': 'windcode',
        's_dq_open': 'open', 's_dq_close': 'close', 's_dq_low': 'low', 's_dq_high': 'high',
        's_dq_volume': 'volume', 's_dq_amount': 'amount'
    }
    full_data.rename(columns=mapping, inplace=True)
    selected_data = full_data.sort_values(by=['windcode', 'trade_dt'], ascending=True)
    return selected_data


def process_spif_data(data: pd.DataFrame):
    mapping = {
        's_info_windcode': 'windcode',
        'trade_dt': 'trade_dt',
        's_dq_open': 'open', 's_dq_close': 'close', 's_dq_low': 'low', 's_dq_high': 'high', 's_dq_settle': 'settle',
        's_dq_volume': 'volume', 's_dq_amount': 'amount', 's_dq_oi': 'oi', 'startdate': 'start_date', 'enddate': 'end_date'
    }

    data = data.rename(columns=mapping)
    data = data.drop(columns=['fs_mapping_windcode', 's_info_windcode_y'])
    selected_data = data.sort_values(by=['trade_dt', 'windcode'], ascending=True)
    return selected_data


def dump_data(data: pd.DataFrame, file_path, file_type="h5"):
    if file_type not in ("h5", 'pkl'):
        raise ValueError(f"Unsupported data type `{file_type}`")

    if os.path.exists(file_path):
        os.remove(file_path)

    if file_type == 'h5':
        # 在这个场景table格式效率更好
        data.to_hdf(file_path, 'df', complevel=9, complib='blosc', format="table")
    elif file_type == 'pkl':
        data.to_pickle(file_path)


def load_data(file_path: str, file_type='h5'):
    if file_type not in ("h5", 'pkl'):
        raise ValueError(f"Unsupported data type `{file_type}`")

    p = pathlib.Path(file_path)
    if p.suffix != '.' + file_type:
        raise ValueError(f"Unmatched data type `{p.suffix}` and `{file_type}`")

    if file_type == 'h5':
        data = pd.read_hdf(file_path)
    elif file_type == 'pkl':
        data = pd.read_pickle(file_path)
    else:
        assert False
    return data


