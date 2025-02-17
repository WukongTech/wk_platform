
from __future__ import annotations

import copy
import pathlib
from typing import Any, MutableMapping

import toml
import fcntl
from wk_util.metaclass import SingletonType
from wk_util.configuration import Configuration


DATA_LAYOUT_SPEC_FILE = 'data_spec.toml'


def combine_into(d: dict, combined: dict) -> None:
    for k, v in d.items():
        if isinstance(v, dict):
            combine_into(v, combined.setdefault(k, {}))
        else:
            combined[k] = v


def merge(left, right, how='left'):
    assert how in ('left', 'right')
    if how == 'left':
        merged_dict = copy.deepcopy(left)
        another_dict = right
    else:
        merged_dict = copy.deepcopy(right)
        another_dict = left

    combine_into(another_dict, merged_dict)
    return merged_dict


def make_patch(ref_dict, updated_dict):
    """
    生成两个字典之间变化的部分，不支持删除
    :param ref_dict:
    :param updated_dict:
    :return:
    """
    result = {}
    fields = set(list(ref_dict.keys()) + list(updated_dict.keys()))
    for k in fields:
        try:
            v = ref_dict[k]
        except KeyError:
            result[k] = updated_dict[k]
            continue

        assert k in updated_dict, 'deletion is not supported'
        # 非同类型即为更新
        if not isinstance(v, type(updated_dict[k])):
            result[k] = updated_dict[k]
            continue

        # 字典进行递归判断
        if isinstance(v, dict):
            value = make_patch(v, updated_dict[k])
            if len(value) > 0:
                result[k] = value
        else:
            if updated_dict[k] != v:
                result[k] = updated_dict[k]
    return result




class DataSpec(metaclass=SingletonType):
    """
    数据文件布局类，暂时仅用于定时任务
    TODO: 在数据处理部分使用此定义文件
    """

    def __init__(self, data_dir: pathlib.Path):
        self.__data_dir = data_dir
        self.__spec_path = data_dir.joinpath(DATA_LAYOUT_SPEC_FILE)
        self.__ref_spec = {}
        self.__spec = None
        try:
            self.load()
        except FileNotFoundError:
            self.__make_empty_data_spec()
            self.load()

    def load(self):
        self.__spec = toml.load(self.__spec_path)
        self.__ref_spec = copy.deepcopy(self.__spec)

    def __make_empty_data_spec(self):
        self.__spec = {
            'daily': {'update_date': '20091231'},
            'yearly_shard': {'update_date': '20091231'},
        }
        self.save()

    def save(self):
        patch = make_patch(self.__ref_spec, self.__spec)
        with self.__spec_path.open('r+') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            current_spec: MutableMapping[str, Any] = toml.load(f)
            f.truncate(0)
            f.seek(0)
            self.__spec = merge(current_spec, patch)
            toml.dump(self.__spec, f)
        self.load()

    @property
    def last_update_date(self):
        return self.__spec['daily']['update_date']

    @last_update_date.setter
    def last_update_date(self, value):
        self.__spec['daily']['update_date'] = value
        self.save()

    def stock_file_name(self, year):
        return f"daily_data_{year}.h5"
        # self.__spec['daily'][f'y{year}']

    def stock_file_path(self, year, check=False):
        """
        check: bool
            检查对应年份文件是否存在
        """
        if check:
            file_name = self.__spec['daily'][f'y{year}']

        return self.__data_dir.joinpath(self.stock_file_name(year))

    def add_stock_file(self, year):
        self.__spec['daily'][f'y{year}'] = self.stock_file_name(year)
        self.save()

    def remove_stock_file(self, year):
        del self.__spec['daily'][f'y{year}']
        self.save()

    def index_file_name(self, windcode):
        return f"daily_data_{windcode}.h5"
        # self.__spec['daily'][f"windcode.{windcode}"]

    def index_file_path(self, windcode, check=False):
        if check:
            file_name = self.__spec['daily'][f'windcode.{windcode}']
        return self.__data_dir.joinpath(self.index_file_name(windcode))

    def add_index_file(self, windcode):
        self.__spec['daily'][f'windcode.{windcode}'] = self.index_file_name(windcode)
        self.save()

    def stock_daily(self):
        return self.__spec['daily']

    @staticmethod
    def spif_file_name(code):
        """
        股指期货（Share Price Index Future）数据文件名
        """
        return f"spif_daily_{code}.h5"

    def spif_file_path(self, code, check=False):
        if check:
            file_name = self.__spec['daily'][f"spif.{code}"]
        return self.__data_dir.joinpath(self.spif_file_name(code))

    def add_spif_file(self, code):
        self.__spec['daily'][f"spif.{code}"] = self.spif_file_name(code)
        self.save()

    @staticmethod
    def factor_exposure_file_name(year):
        return f"factor_exposure_{year}.h5"

    def factor_exposure_file_path(self, year, check=False):
        # if check:
            # file_name = self.__spec['daily'][f"spif.{code}"]
        return self.__data_dir.joinpath(self.factor_exposure_file_name(year))

    def check_data_shard_spec(self):
        try:
            d = self.__spec['data_shard']
        except KeyError:
            self.__spec['data_shard'] = {}

    def add_shard_data_spec(self, data_name, type_):
        """
        在配置文件中创建字段
        """
        assert type_ in ('code_shard', 'yearly_shard')
        self.check_data_shard_spec()
        try:
            d = self.__spec['data_shard'][data_name]
        except KeyError:
            self.__spec['data_shard'][data_name] = {"shard_type": type_}

    def shard_spec(self, data_name):
        return self.__spec['data_shard'][data_name]

    def get_shard_update_date(self, data_name):
        return self.shard_spec(data_name)[f"{data_name}_update"]

    def set_shard_update_date(self, data_name, date_str):
        self.shard_spec(data_name)[f"{data_name}_update"] = date_str
        self.save()

    @classmethod
    def shard_file_name(cls, data_name, aux_info, file_type='h5'):
        assert file_type in ('h5', 'pkl')
        if file_type == 'h5':
            return f"{cls.shard_field_name(data_name, aux_info)}.h5"
        elif file_type == 'pkl':
            return f"{cls.shard_field_name(data_name, aux_info)}.pkl"

    @staticmethod
    def shard_field_name(data_name, aux_info):
        return f'{data_name}_{aux_info}'

    def yearly_shard_file_path(self, table_name, year, check=False, file_type='h5'):
        """
        check: bool
            检查对应年份文件是否存在
        """
        if check:
            file_name = self.shard_spec(table_name)[self.shard_field_name(table_name, year)]

        return self.__data_dir.joinpath(self.shard_file_name(table_name, year, file_type=file_type))

    def add_yearly_shard_file(self, table_name, year, file_type='h5'):
        file_name = self.shard_file_name(table_name, year, file_type=file_type)
        self.shard_spec(table_name)[self.shard_field_name(table_name, year)] = file_name
        self.save()
        return file_name

    def remove_yearly_shard_file(self, table_name, year):
        del self.shard_spec(table_name)[self.shard_field_name(table_name, year)]
        self.save()

    def code_shard_file_path(self, data_name, code, check=False, file_type='h5'):
        """
        check: bool
            检查对应年份文件是否存在
        """
        if check:
            file_name = self.shard_spec(data_name)[
                self.shard_field_name(data_name, code)
            ]

        return self.__data_dir.joinpath(
            self.shard_file_name(data_name, code, file_type=file_type)
        )

    def add_code_shard_file(self, data_name, code, file_type='h5'):
        file_name = self.shard_file_name(data_name, code, file_type=file_type)
        self.shard_spec(data_name)[
            self.shard_field_name(data_name, code)
        ] = file_name
        self.save()
        return file_name

    # def remove_code_shard_file(self, data_name, code):
    #     del self.shard_spec(data_name)[self.shard_file_name(data_name, code)]
    #     self.save()


data_spec = DataSpec(Configuration().data_dir)