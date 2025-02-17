# -*- coding: utf-8 -*-
"""
Created on Wed Aug 09 11:22:37 2017

@author: cxd

Modified on 20211227

@author: lx
"""
from __future__ import annotations

import pathlib
# from typing import Dict, TypeVar
import datetime
from dataclasses import dataclass
from functools import lru_cache

import toml
import pandas as pd
from pandarallel import pandarallel
from pyalgotrade.bar import Frequency

from wk_data import db_util
from wk_data.exceptions import DataOutOfRangeException
from wk_util.configuration import Configuration
from wk_util.metaclass import SingletonType
from wk_data.proc_util import process_price_data_procedure, process_index_data, dump_data
import wk_data.proc_util as pu
from wk_util.data import symbol_trans, CodePattern
from wk_data.mappings import register_get, register_get_no_arg
from wk_data.data.mr_data import get_merger_reorganization_data, MRRecord
from wk_util.logger import console_log
from wk_db.funcs import read_sql
import wk_db
from sqlalchemy import text
from wk_data.data_spec import DataSpec






# class PriceDataSource:
#     """
#     连接数据
#     """
#
#     def __init__(self, config):
#         self._config = config
#
#     def sql_engine(self):
#         pass
#
#     def sql_select_all(self, begin_date, end_date=None):
#         """
#         获取表内所有数据
#         """
#         db = db_util.DBComOrc('wktz', self._config)
#         # print('init wktz')
#         fields = [
#             "trade_dt", "s_info_windcode",
#             "s_info_name", "s_info_listdate", "s_info_delistdate", "s_info_st",
#             "s_info_citics1_name", "s_info_suspension",
#             "s_info_limit", "s_dq_open", "s_dq_close",
#             "s_dq_low", "s_dq_high", "s_dq_volume",
#             "s_dq_amount", "s_dq_adjfactor"
#         ]
#
#         if end_date is not None:
#             console_log(f"fetching data from {begin_date} to {end_date}")
#             sql = f"select {', '.join(fields)} from asharedailystatus where trade_dt >= {begin_date} and trade_dt < {end_date}"
#
#         else:
#             console_log(f"fetching data from {begin_date}")
#             sql = f"select {', '.join(fields)} from asharedailystatus where trade_dt >= {begin_date}"
#
#         # dat = pd.read_sql_query(sql, self.__engine, index_col = None)
#         daily_df, tmp1, tmp2 = db.sqlexecute(sql)
#         daily_df = pd.DataFrame(daily_df, columns=fields)
#         return daily_df
#
#     def sql_select_count(self, begin_date, end_date=None):
#         """
#         获取表内所有数据
#         """
#         db = db_util.DBComOrc('wktz', self._config)
#
#         if end_date is not None:
#             console_log(f"counting data from {begin_date} to {end_date}")
#             sql = f"select count(*) from asharedailystatus where trade_dt >= {begin_date} and trade_dt < {end_date}"
#
#         else:
#             console_log(f"counting data from {begin_date}")
#             sql = f"select count(*) from asharedailystatus where trade_dt >= {begin_date}"
#
#         # dat = pd.read_sql_query(sql, self.__engine, index_col = None)
#         daily_df, tmp1, tmp2 = db.sqlexecute(sql)
#         # daily_df = pd.DataFrame(daily_df, columns=fields)
#         return daily_df[0]
#
#     def sql_select_instrument(self, windcode):
#         db = db_util.DBComOrc('cms', self._config)
#         # print('init wktz')
#         fields = [
#             "trade_dt", "s_info_windcode", "s_dq_open", "s_dq_close",
#             "s_dq_low", "s_dq_high", "s_dq_volume",
#             "s_dq_amount"
#         ]
#         sql = f"select {', '.join(fields)} from wind_admin.aindexeodprices where s_info_windcode='{windcode}'"
#
#         # dat = pd.read_sql_query(sql, self.__engine, index_col = None)
#         daily_df, tmp1, tmp2 = db.sqlexecute(sql)
#         daily_df = pd.DataFrame(daily_df, columns=fields)
#         return daily_df
#
#     def sql_select_trade_calendar(self, begin_date, end_date=None):
#         """
#         使用000001.SH的交易日作为交易日历
#         """
#         # db = db_util.DBComOrc('wktz', self._config)
#         #
#         # fields = [
#         #     "trade_days", "s_info_exchmarket"
#         # ]
#
#         # if end_date is not None:
#         #     console_log(f"fetching trade calendar from {begin_date} to {end_date}")
#         #     sql = f"select {', '.join(fields)} from asharecalendarzl where trade_days >= {begin_date} and trade_days < {end_date} and s_info_exchmarket='SZSE'"
#         #
#         # else:
#         #     console_log(f"fetching trade calendar from {begin_date}")
#         #     sql = f"select {', '.join(fields)} from asharecalendarzl where trade_days >= {begin_date} and s_info_exchmarket='SZSE'"
#
#         db = db_util.DBComOrc('cms', self._config)
#         fields = [
#             "trade_dt", "s_info_windcode",
#         ]
#         if end_date is not None:
#             console_log(f"fetching trade calendar from {begin_date} to {end_date}")
#             sql = f"select {', '.join(fields)} from wind_admin.aindexeodprices where s_info_windcode='000001.SH' and trade_dt >= {begin_date} and trade_dt < {end_date}"
#
#         else:
#             console_log(f"fetching trade calendar from {begin_date}")
#             sql = f"select {', '.join(fields)} from wind_admin.aindexeodprices where s_info_windcode='000001.SH' and trade_dt >= {begin_date}"
#
#         # dat = pd.read_sql_query(sql, self.__engine, index_col = None)
#         daily_df, tmp1, tmp2 = db.sqlexecute(sql)
#         daily_df = pd.DataFrame(daily_df, columns=fields)
#         daily_df.rename(columns={
#             "trade_dt": "trade_days"
#         }, inplace=True)
#         return daily_df


def get_trade_calendar(begin_date, end_date=None):
    config = Configuration()

    fields = [
        "trade_dt", "s_info_windcode",
    ]
    if end_date is not None:
        console_log(f"fetching trade calendar from {begin_date} to {end_date}")
        sql = f"select {', '.join(fields)} from aindexeodprices where s_info_windcode='000001.SH' and trade_dt >= {begin_date} and trade_dt < {end_date}"

    else:
        console_log(f"fetching trade calendar from {begin_date}")
        sql = f"select {', '.join(fields)} from aindexeodprices where s_info_windcode='000001.SH' and trade_dt >= {begin_date}"


    data = read_sql(sql)
    data.rename(columns={"trade_dt": "trade_days"}, inplace=True)

    return sorted(data['trade_days'].tolist())


def get_index_data(windcode):
    config = Configuration()
    # fin = PriceDataSource(config)
    # fin.sql_engine()
    # data = fin.sql_select_instrument(windcode)

    fields = [
        "trade_dt", "s_info_windcode", "s_dq_open", "s_dq_close",
        "s_dq_low", "s_dq_high", "s_dq_volume",
        "s_dq_amount"
    ]
    data = wk_db.read_sql(
        f"select {', '.join(fields)} from AINDEXEODPRICES where s_info_windcode=:windcode",
        sql_params={'windcode': windcode},
        # db_loc=config.data_src
    )

    # dat = pd.read_sql_query(sql, self.__engine, index_col = None)
    # daily_df, tmp1, tmp2 = db.sqlexecute(sql)
    # daily_df = pd.DataFrame(daily_df, columns=fields)
    return data



def get_price_data(begin_date, end_date=None):
    config = Configuration()

    fields = [
        "trade_dt", "s_info_windcode",
        "s_info_name", "s_info_listdate", "s_info_delistdate", "s_info_st",
        "s_info_citics1_name", "s_info_suspension",
        "s_info_limit", "s_dq_open", "s_dq_close",
        "s_dq_low", "s_dq_high", "s_dq_volume",
        "s_dq_amount", "s_dq_adjfactor"
    ]

    if end_date is not None:
        console_log(f"fetching data from {begin_date} to {end_date}")
        sql = f"select {', '.join(fields)} from asharedailystatus where trade_dt >= {begin_date} and trade_dt < {end_date}"

    else:
        console_log(f"fetching data from {begin_date}")
        sql = f"select {', '.join(fields)} from asharedailystatus where trade_dt >= {begin_date}"

    data = read_sql(sql, db_loc=config.data_src)
    return data


def get_price_data_count(begin_date, end_date=None):
    config = Configuration()
    if end_date is not None:
        console_log(f"counting data from {begin_date} to {end_date}")
        sql = f"select count(*) from asharedailystatus where trade_dt >=:begin_date and trade_dt <:end_date"
        sql_params = {'begin_date': begin_date, 'end_date': end_date}
    else:
        console_log(f"counting data from {begin_date}")
        sql = f"select count(*) from asharedailystatus where trade_dt >=:begin_date"
        sql_params = {'begin_date': begin_date}

    daily_df = wk_db.read_sql(sql, sql_params=sql_params, db_loc=config.data_src)
    return daily_df['COUNT(*)'][0]


def get_spif_data(code):
    """
    获取当月主力合约日频数据
    """
    config = Configuration()

    mapping_field = [
        's_info_windcode',
        'fs_mapping_windcode',
        'startdate',
        'enddate',
    ]

    market_data_field = [
        's_info_windcode',
        'trade_dt',
        's_dq_open',
        's_dq_high',
        's_dq_low',
        's_dq_close',
        's_dq_settle',
        's_dq_volume',
        's_dq_amount',
        's_dq_oi',
    ]

    # engine = db_util.create_oracle_engine(config, 'cms')
    # with engine.connect() as conn:
    #     map_data = pd.read_sql(
    #         f"select {','.join(mapping_field)} from CfuturesContractMapping where s_info_windcode='{code}00.CFE' order by enddate",
    #         con=conn
    #     )
    #
    #     market_data = pd.read_sql(
    #         f"select {','.join(market_data_field)} from CINDEXFUTURESEODPRICES where substr(S_INFO_WINDCODE, 0, 2) = '{code}' and length(S_INFO_WINDCODE) = 10",
    #         con=conn
    #     )
    #
    #     data = market_data.merge(map_data, left_on='s_info_windcode', right_on='fs_mapping_windcode')

    map_data = wk_db.read_sql(
        f"select {','.join(mapping_field)} from CfuturesContractMapping where s_info_windcode=:windcode order by enddate",
        sql_params={'windcode': f'{code}00.CFE'},
        # db_loc=config.data_src
    )

    market_data = wk_db.read_sql(
        f"select {','.join(market_data_field)} from CINDEXFUTURESEODPRICES where substr(S_INFO_WINDCODE, 0, 2)=:code and length(S_INFO_WINDCODE) = 10",
        sql_params={'code': code},
        # db_loc=config.data_src
    )
    data = market_data.merge(map_data, left_on='s_info_windcode', right_on='fs_mapping_windcode')
    data.rename(columns={'s_info_windcode_x': 's_info_windcode'}, inplace=True)
    return data


def get_index_components(windcode, trade_dt):
    """
    获取指数成分股权重
    """
    stmt = text("select * from date_mapping where date=:date").bindparams(date=trade_dt)
    date_mapping_df = read_sql(stmt, db_loc='quant_data')
    last_index_dt = date_mapping_df['last_index_dt'].tolist()[0]
    fields = [
        's_con_windcode',
        'i_weight',
    ]
    stmt = text(f"select {','.join(fields)} from aindexhs300freeweight where s_info_windcode=:windcode and trade_dt=:trade_dt")
    stmt = stmt.bindparams(windcode=windcode, trade_dt=last_index_dt)
    data = read_sql(stmt, db_loc='quant_data')
    return data


def get_a_share_eod_derivative_indicator(trade_dt):
    fields = [
        's_info_windcode',
        's_val_mv'
    ]
    stmt = text(
        f"select {','.join(fields)} from AShareEODDerivativeIndicator where trade_dt=:trade_dt")
    stmt = stmt.bindparams(trade_dt=trade_dt)
    data = read_sql(stmt, db_loc='quant_data')
    return data


def date_range_to_year_range(begin_date, end_date=None):
    begin_year = int(begin_date[:4])
    end_year = datetime.datetime.now().year
    if end_date is None:
        end_year = int(end_date[:4])
    assert begin_year <= end_year
    return list(range(begin_year, end_year + 1))


class DataSourceBase(metaclass=SingletonType):
    """
    数据布局
    [daily]
    update_date = 20211229
    y2015 = “daily_data_2015.h5"
    y2016 = “daily_data_2016.h5"
    "windcode.000001.SH" = "daily_data_000001.SH.h5"
    ...

    """

    def __init__(self, fetch=False):
        self.__pandarallel_initialized = False
        self.__config = Configuration()
        # self.__spec_path = self.__config.data_dir.joinpath(DATA_LAYOUT_SPEC_FILE)
        self.__data_spec = DataSpec(self.__config.data_dir)
        self.__data_dir = self.__config.data_dir
        self.__fetch = fetch  # 标记是否允许访问数据库更新数据
        # try:
        #     self.__data_layout_spec = toml.load(self.__spec_path)
        # except FileNotFoundError:
        #     self.__make_empty_data_spec()
        #     self.__data_layout_spec = toml.load(self.__spec_path)
        self.__mr_map: dict[str, MRRecord] = {}
        self.__build_mr_map()

        # self.__data_cache = None

    def __build_mr_map(self):
        self.__mr_map.clear()
        for t in get_merger_reorganization_data(original=True).itertuples():
            self.__mr_map[t.old_windcode] = MRRecord(str(t.change_dt), t.new_windcode, t.ratio)

        console_log('total merger & reorganization record', len(self.__mr_map))

    def get_trade_calendar(self, begin_date, end_date=None):
        return get_trade_calendar(begin_date, end_date)

    def __download_daily_data(self, year):
        if not self.__pandarallel_initialized:
            pandarallel.initialize()
            self.__pandarallel_initialized = True

        console_log("downloading daily data of", year)
        begin_date = f"{year}0101"
        end_date = f"{year + 1}0101"
        trade_calendar = get_trade_calendar(begin_date, end_date)
        data = get_price_data(begin_date, end_date)

        previous_file_name = f"daily_data_{year - 1}.h5"
        try:
            previous_file_name = self.__data_spec.stock_file_path(year - 1, check=True)
            previous_data = pd.read_hdf(previous_file_name)
            data = process_price_data_procedure(data, previous_data, trade_calendar, self.__mr_map)
        except KeyError:
            console_log(f'{previous_file_name} does not exist')
            data = process_price_data_procedure(data, previous_data=None, trade_calendar=trade_calendar,
                                                mr_map=self.__mr_map)

        file_name = f"daily_data_{year}.h5"
        dump_data(data, self.__config.data_dir.joinpath(file_name))
        self.__data_spec.add_stock_file(year)
        self.__data_spec.last_update_date = data['trade_dt'].max()
        # self.__data_layout_spec['daily'][f"y{year}"] = file_name
        # with open(self.__spec_path, 'w') as f:
        #     toml.dump(self.__data_layout_spec, f)

    def __prepare_data(self, begin_date, end_date=None):
        begin_year = int(begin_date[:4])
        end_year = datetime.datetime.now().year
        if end_date is not None:
            end_year = int(end_date[:4])
        assert begin_year <= end_year

        file_names = []
        # daily_spec = self.__data_layout_spec['daily']
        # daily_spec = self.__data_spec.stock_daily()
        for year in range(begin_year, end_year + 1):
            try:
                # file_path = self.__data_dir.joinpath(daily_spec[f'y{year}'])
                file_path = self.__data_spec.stock_file_path(year, check=True)
            except KeyError:
                if not self.__fetch:
                    raise DataOutOfRangeException("包含时间范围外的数据")
                else:
                    self.__download_daily_data(year)
                    # daily_spec = self.__data_layout_spec['daily']
                    # file_path = self.__data_dir.joinpath(daily_spec[f'y{year}'])
                    file_path = self.__data_spec.stock_file_path(year, check=True)
            file_names.append(file_path)
        return file_names

    @lru_cache(maxsize=10)
    def __read_hdf(self, f, **kwargs):
        return pd.read_hdf(f, **kwargs)

    def __load_data(self, file_names):
        results = []
        for f in file_names:
            data = self.__read_hdf(f)
            results.append(data)
        return pd.concat(results)

    def get_daily(self, begin_date, end_date=None):
        console_log("loading market data...")
        file_names = self.__prepare_data(begin_date, end_date)
        data = self.__load_data(file_names)
        if end_date is None:
            return data[data['trade_dt'] >= begin_date]
        else:
            return data[(data['trade_dt'] >= begin_date) & (data['trade_dt'] <= end_date)]

    def fetch_daily_data(self, begin_date, end_date=None):
        console_log("fetching data...")
        file_names = self.__prepare_data(begin_date, end_date)

    def __download_index_data(self, windcode):
        print("downloading index data of", windcode)
        data = get_index_data(windcode)
        data = process_index_data(data)
        file_name = f"daily_data_{windcode}.h5"
        dump_data(data, self.__config.data_dir.joinpath(file_name))
        self.__data_spec.add_index_file(windcode)
        # self.__data_layout_spec['daily'][f"windcode.{windcode}"] = file_name
        # with open(self.__spec_path, 'w') as f:
        #     toml.dump(self.__data_layout_spec, f)

    def __prepare_index_data(self, windcode):
        file_names = []
        # daily_spec = self.__data_layout_spec['daily']
        try:
            # file_path = self.__data_dir.joinpath(daily_spec[f'windcode.{windcode}'])
            file_path = self.__data_spec.index_file_path(windcode, check=True)
        except KeyError:
            if not self.__fetch:
                raise DataOutOfRangeException(f"未找到{windcode}的数据")
            else:
                self.__download_index_data(windcode)
                # daily_spec = self.__data_layout_spec['daily']
                # file_path = self.__data_dir.joinpath(daily_spec[f'windcode.{windcode}'])
                file_path = self.__data_spec.index_file_path(windcode, check=True)
        file_names.append(file_path)
        return file_names

    def get_index_daily(self, windcode, begin_date=None, end_date=None):
        file_names = self.__prepare_index_data(windcode)
        data = self.__load_data(file_names)
        # return data
        # print(data.shape)
        if begin_date is None:
            return data
        if end_date is None:
            return data[data['trade_dt'] >= begin_date]
        else:
            return data[(data['trade_dt'] >= begin_date) & (data['trade_dt'] <= end_date)]

    def __download_spif_data(self, code):
        print("downloading spif data of", code)
        data = get_spif_data(code)
        data = pu.process_spif_data(data)
        pu.dump_data(data, self.__data_spec.spif_file_path(code, check=False))
        self.__data_spec.add_spif_file(code)

    def __prepare_spif_data(self, code):
        file_names = []
        try:
            file_path = self.__data_spec.spif_file_path(code, check=True)
        except KeyError:
            if not self.__fetch:
                raise DataOutOfRangeException(f"未找到{code}的数据")
            else:
                self.__download_spif_data(code)
                file_path = self.__data_spec.spif_file_path(code, check=False)
        file_names.append(file_path)
        return file_names

    def get_spif_daily(self, code, begin_date=None, end_date=None):
        file_names = self.__prepare_spif_data(code)
        data = self.__load_data(file_names)
        if begin_date is None:
            return data
        if end_date is None:
            return data[data['trade_dt'] >= begin_date]
        else:
            return data[(data['trade_dt'] >= begin_date) & (data['trade_dt'] <= end_date)]

    def get_mr_map(self):
        return self.__mr_map

    def reset(self):
        self.__data_cache = None


class DataSource:
    """
    转发对数据源的调用，便于自动提示
    """

    def __init__(self, fetch=False):
        self.data_source = DataSourceBase(fetch)

    def get_daily(self, begin_date, end_date=None) -> pd.DataFrame:
        return self.data_source.get_daily(begin_date, end_date)

    def get_data(self, begin_date, end_date=None, frequency=Frequency.DAY) -> pd.DataFrame:
        assert frequency == Frequency.DAY
        return self.get_daily(begin_date, end_date)

    def fetch_daily_data(self, begin_date, end_date=None):
        self.data_source.fetch_daily_data(begin_date, end_date)

    def get_spif_data(self, code, begin_date, end_date=None) -> pd.DataFrame:
        return self.data_source.get_spif_daily(code, begin_date, end_date)

    def get_index_data(self, instrument: None | str = None, begin_date=None, end_date=None) -> pd.DataFrame:
        if instrument is not None:
            assert isinstance(instrument, str)
            data = self.data_source.get_index_daily(instrument, begin_date, end_date)
            return data
        default_list = ["000001.SH", "000016.SH", "000300.SH", "399905.SZ", "000852.SH", "399006.SZ"]
        data_list = []
        for inst in default_list:
            data_list.append(self.data_source.get_index_daily(inst, begin_date, end_date))

        # 使用399905.SZ的数据代替000905.SH
        data = self.data_source.get_index_daily("399905.SZ", begin_date, end_date).copy()
        data['windcode'] = '000905.SH'
        data_list.append(data)

        data = pd.concat(data_list)
        return data

    def get_mr_data(self):
        """
        获取并购重组后的代码、股价变动信息
        """
        return self.data_source.get_mr_map()

    def get_trade_calendar(self, begin_date, end_date=None):
        """
        获取交易日历，从指数行情计算得到
        """
        data = self.data_source.get_index_daily('000001.SH', begin_date, end_date).sort_values('trade_dt')
        trade_calendar = data['trade_dt'].tolist()
        return trade_calendar

    def reset(self):
        self.data_source.reset()


class BenchDataSource:
    def __init__(self, fetch=False):
        self.data_source = DataSourceBase(fetch)

    def get_daily(self, instrument, begin_date=None, end_date=None) -> pd.DataFrame:
        data = self.data_source.get_index_daily(instrument, begin_date, end_date)
        # t = data.head()
        # t = (data['windcode'] == '000001.SZ')
        return data


class RiskFactorDataSource(metaclass=SingletonType):
    def __init__(self):
        self.__config = Configuration()
        # self.__factor_dir = self.__config.risk_factor_dir
        self.__data_spec = DataSpec(self.__config.data_dir)
        # print()

    def get_factor_exposure(self, trade_dt) -> pd.DataFrame:
        year = trade_dt[:4]
        file_path = (self.__data_spec.factor_exposure_file_path(year))
        data = pd.read_hdf(self.__data_spec.factor_exposure_file_path(year))

        return data[data['date'] == trade_dt]

    def get_index_stock_weight(self, windcode, trade_dt) -> pd.DataFrame:
        """
        获取指数在某个交易日的个股权重
        """
        data = get_index_components(windcode, trade_dt)
        data.rename(columns={
            's_con_windcode': 'windcode',
            'i_weight': 'weight'
        }, inplace=True)
        data['weight'] = data['weight'] / 100
        return data

    def get_market_value(self, trade_dt) -> pd.DataFrame:
        data = get_a_share_eod_derivative_indicator(trade_dt)
        data.rename(columns={
            's_info_windcode': 'windcode',
            's_val_mv': 'market_value'
        }, inplace=True)
        return data


