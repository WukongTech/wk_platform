# import abc
import datetime

import pandas as pd
# from pandarallel import pandarallel
# from sqlalchemy import text

from wk_util.configuration import Configuration
from wk_util.data import filter_data
from wk_util.logger import console_log
# from wk_util.metaclass import SingletonType
from wk_data.data_spec import DataSpec
from wk_data.exceptions import DataOutOfRangeException
from wk_data.proc_util import load_data, dump_data
import wk_db


class ShardDataBase:
    def __init__(self, file_type):
        assert file_type in ('h5', 'pkl')
        self.__file_type = file_type

    @property
    def file_type(self):
        return self.__file_type

    def load_data(self, file_names):
        results = []
        for f in file_names:
            data: pd.DataFrame = load_data(f, self.__file_type)
            results.append(data)
        return pd.concat(results)


class YearlyShardData(ShardDataBase):
    """
    根据数据对应年份切片缓存在文件中
    """
    def __init__(self, table_name, *,
                 data_name=None,
                 fields=None,  date_field='trade_dt', fetch=False, fetch_func=None, proc_func=None, use_prev_data=False,
                 retrospect=False,
                 file_type='h5'):
        super().__init__(file_type)
        self.__data_name = data_name
        if self.__data_name is None:
            self.__data_name = table_name

        self.__table_name = table_name
        self.__date_field = date_field
        self.__fields = fields
        self.__fetch = fetch
        self.__config = Configuration()
        self.__data_spec = DataSpec(self.__config.data_dir)
        self.__fetch_func = fetch_func
        self.__proc_func = proc_func
        self.__use_prev_data = use_prev_data
        self.__retrospect = retrospect

        if self.__retrospect:
            assert self.__use_prev_data

        if fetch:
            if self.__fetch_func is None:
                assert fields is not None
                self.__fetch_func = self.__gen_fetch_func()

        self.__pandarallel_initialized = False

    def __gen_fetch_func(self):
        def fetch_func(begin_date, end_date):
            # stmt = text(
            #     f"select {','.join(self.__fields)} from {self.__table_name} where {self.__date_field}>=:begin_date and {self.__date_field}<=:end_date")
            # stmt = stmt.bindparams(begin_date=begin_date, end_date=end_date)
            stmt = f"select {','.join(self.__fields)} from {self.__table_name} " \
                   f"where {self.__date_field} >= {begin_date} and {self.__date_field} <= {end_date}"
            # print(stmt)
            data = wk_db.read_sql(stmt)
            return data
        return fetch_func

    def fetch_data(self, begin_date, end_date=None):
        console_log(f"loading {self.__data_name} data from table {self.__table_name}...")
        file_names = self.__prepare_data(begin_date, end_date)
        data = self.load_data(file_names)
        if end_date is None:
            return data[data[self.__date_field] >= begin_date]
        else:
            return data[(data[self.__date_field] >= begin_date) & (data[self.__date_field] <= end_date)]

    def __prepare_data(self, begin_date, end_date=None):
        begin_year = int(begin_date[:4])
        end_year = datetime.datetime.now().year
        if end_date is not None:
            end_year = int(end_date[:4])
        assert begin_year <= end_year

        file_names = []
        for year in range(begin_year, end_year + 1):
            try:
                file_path = self.__data_spec.yearly_shard_file_path(self.__data_name, year,
                                                                    check=True, file_type=self.file_type)
            except KeyError:
                if not self.__fetch:
                    raise DataOutOfRangeException("data fetch is not allowed, please set `fetch=True`")
                else:
                    self.__download_data(year)
                    file_path = self.__data_spec.yearly_shard_file_path(self.__data_name, year,
                                                                        check=True, file_type=self.file_type)
            file_names.append(file_path)
        return file_names

    def __force_prepare_data(self, begin_date, end_date=None):
        assert self.__fetch is True
        begin_year = int(begin_date[:4])
        end_year = datetime.datetime.now().year
        if end_date is not None:
            end_year = int(end_date[:4])
        assert begin_year <= end_year

        modified_file_path = []
        for year in range(begin_year, end_year + 1):
            path_data = self.__download_data(year)
            modified_file_path = modified_file_path + path_data
        return list(set(modified_file_path))

    def __call_proc_func(self, data, year, buf_begin_date):
        if self.__proc_func is None:
            return data

        begin_date = f"{year}0101"
        end_date = f"{year}1231"

        if self.__use_prev_data:
            try:
                file_path = self.__data_spec.yearly_shard_file_path(
                    self.__data_name, year - 1, check=True, file_type=self.file_type
                )
                prev_data = self.load_data([file_path])
                if self.__retrospect:
                    processed_data = self.__proc_func(prev_data.copy(), data, year)
                    prev_data = pd.concat([
                        filter_data(prev_data, prev_data[self.__date_field].min(), buf_begin_date, close_right=False),
                        filter_data(processed_data, buf_begin_date, f"{year - 1}1231", date_field_tag=self.__date_field)
                    ])
                    current_data = filter_data(processed_data, begin_date, end_date, date_field_tag=self.__date_field)
                    return current_data, prev_data
                else:
                    processed_data = self.__proc_func(prev_data, data, year)
                    processed_data = filter_data(processed_data, begin_date, end_date, date_field_tag=self.__date_field)
                    return processed_data, None
            except KeyError:
                processed_data = self.__proc_func(None, data, year)
                processed_data = filter_data(processed_data, begin_date, end_date, date_field_tag=self.__date_field)
                return processed_data, None

        else:
            processed_data = self.__proc_func(data, year)
            return processed_data, None

    def __download_data(self, year):
        # if not self.__pandarallel_initialized:
        #     pandarallel.initialize()
        #     self.__pandarallel_initialized = True

        console_log(f"downloading yearly shard data `{self.__data_name}` of", year)
        buf_begin_date = f"{year-1}1220"
        begin_date = f"{year}0101"
        end_date = f"{year}1231"
        if self.__retrospect:
            data = self.__fetch_func(buf_begin_date, end_date)
        else:
            data = self.__fetch_func(begin_date, end_date)

        data, prev_data = self.__call_proc_func(data, year, buf_begin_date)

        file_path_list = []

        file_path = self.__data_spec.yearly_shard_file_path(self.__data_name, year, file_type=self.file_type)
        file_path_list.append(str(file_path.absolute()))
        dump_data(data, file_path, self.file_type)
        if self.__retrospect and prev_data is not None:
            file_path = self.__data_spec.yearly_shard_file_path(self.__data_name, year-1, file_type=self.file_type)
            dump_data(prev_data, file_path, self.file_type)
            file_path_list.append(str(file_path.absolute()))
        try:
            self.__data_spec.add_yearly_shard_file(self.__data_name, year, file_type=self.file_type)
        except KeyError:
            self.__data_spec.add_shard_data_spec(self.__data_name, type_='yearly_shard')
            self.__data_spec.add_yearly_shard_file(self.__data_name, year, file_type=self.file_type)
        self.__data_spec.set_shard_update_date(self.__data_name, data[self.__date_field].max())
        console_log('set date', self.__data_spec.get_shard_update_date(self.__data_name))
        return file_path_list

    def update(self, begin_date='20100101'):
        """
        将数据更新至当前时间
        """
        current_time = datetime.datetime.now()

        try:
            begin_date = (
                    datetime.datetime.strptime(self.__data_spec.get_shard_update_date(self.__data_name), "%Y%m%d")
                    + datetime.timedelta(days=1)
            ).strftime("%Y%m%d")
        except KeyError:
            pass

        end_date_next = (current_time + datetime.timedelta(days=1)).strftime("%Y%m%d")

        # 同一天多次运行时避免最新的数据重复
        if end_date_next <= begin_date:
            return []

        return self.__force_prepare_data(begin_date=begin_date)


class CodeShardData(ShardDataBase):
    """
    根据windcode切片缓存在文件中
    """
    def __init__(self, code_list, table_name, *,
                 data_name=None,
                 fields=None,  date_field='trade_dt', fetch=False, fetch_func=None, proc_func=None,
                 file_type='h5'):
        super(CodeShardData, self).__init__(file_type)
        self.__code_list = code_list
        self.__data_name = data_name
        if self.__data_name is None:
            self.__data_name = table_name

        self.__table_name = table_name
        self.__date_field = date_field
        self.__fields = fields
        self.__fetch = fetch
        self.__config = Configuration()
        self.__data_spec = DataSpec(self.__config.data_dir)
        self.__fetch_func = fetch_func
        self.__proc_func = proc_func

        if fetch:
            if self.__fetch_func is None:
                assert fields is not None
                self.__fetch_func = self.__gen_fetch_func()

    def __gen_fetch_func(self):
        def fetch_func(code):
            data = wk_db.read_sql(
                f"select {','.join(self.__fields)} from {self.__table_name} where s_info_windcode=:windcode",
                sql_params={'windcode': code},
                # db_loc=config.data_src
            )
            return data
        return fetch_func

    def fetch_data(self, begin_date, end_date=None, *, instrument=None):
        if instrument is not None:
            code_list = [instrument]
        else:
            code_list = self.__code_list
        data_list = []
        for code in code_list:
            file_names = self.__prepare_data(code)
            data = self.load_data(file_names)
            if end_date is None:
                data_part = data[data[self.__date_field] >= begin_date]
            else:
                data_part = data[(data[self.__date_field] >= begin_date) & (data[self.__date_field] <= end_date)]
            data_list.append(data_part)
        return pd.concat(data_list)

    def __prepare_data(self, windcode):
        file_names = []
        try:
            file_path = self.__data_spec.code_shard_file_path(self.__data_name, windcode, check=True,
                                                              file_type=self.file_type)
        except KeyError:
            if not self.__fetch:
                raise DataOutOfRangeException(f"未找到{windcode}的数据")
            else:
                self.__download_data(windcode)
                file_path = self.__data_spec.code_shard_file_path(self.__data_name, windcode, check=True,
                                                                  file_type=self.file_type)
        file_names.append(file_path)
        return file_names

    def __force_prepare_data(self):

        modified_file_path = []
        for code in self.__code_list:
            path_data = self.__download_data(code)
            modified_file_path.append(path_data)
        return modified_file_path

    def __download_data(self, code):
        # if not self.__pandarallel_initialized:
        #     pandarallel.initialize()
        #     self.__pandarallel_initialized = True

        console_log(f"downloading code shard data `{self.__data_name}` of", code)

        data = self.__fetch_func(code)
        if self.__proc_func:
            data = self.__proc_func(data)

        file_path = self.__data_spec.code_shard_file_path(self.__data_name, code, file_type=self.file_type)
        dump_data(data, file_path, self.file_type)
        try:
            self.__data_spec.add_code_shard_file(self.__data_name, code, file_type=self.file_type)
        except KeyError:
            self.__data_spec.add_shard_data_spec(self.__data_name, type_='code_shard')
            self.__data_spec.add_code_shard_file(self.__data_name, code, file_type=self.file_type)
        self.__data_spec.set_shard_update_date(self.__data_name, data[self.__date_field].max())
        return str(file_path.absolute())

    def update(self, begin_date='20100101'):
        """
        将数据更新至当前时间
        """
        current_time = datetime.datetime.now()

        try:
            begin_date = (
                    datetime.datetime.strptime(self.__data_spec.get_shard_update_date(self.__data_name), "%Y%m%d")
                    + datetime.timedelta(days=1)
            ).strftime("%Y%m%d")
        except KeyError:
            pass

        end_date_next = (current_time + datetime.timedelta(days=1)).strftime("%Y%m%d")
        console_log(f"update range {begin_date} {end_date_next} ")

        # 同一天多次运行时避免最新的数据重复
        if end_date_next <= begin_date:
            return []

        return self.__force_prepare_data()
