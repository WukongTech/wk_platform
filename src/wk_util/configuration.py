import pathlib
from typing import Iterable

import toml
import platform
import copy
import pydash
from wk_util.metaclass import SingletonType
from wk_util.logger import console_log

# class DataConfiguration(metaclass=SingletonType):
#     """
#     保存数据路径
#     """
#
#     def __init__(self, bench_dir: str = "benchFile", result_dir: str = "result"):
#         # self.__market_dir = pathlib.Path(market_dir)
#         self.__bench_dir = pathlib.Path(bench_dir)
#         self.__result_dir = pathlib.Path(result_dir)
#
#     # @property
#     # def market_dir(self):
#     #     return self.__market_dir
#
#     @property
#     def bench_dir(self):
#         return self.__bench_dir
#
#     @property
#     def result_dir(self):
#         return self.__result_dir

DATA_VERSION = "v0.2"

DEFAULT_CONFIG = {
    "store": {
        "data_dir": "/data/v0.1/data",
        "data_dir2": "/data",
    },
    "wk_trade_toolkit": {
        "data_src": "wktz"
    }
}


if platform.system() == "Windows":
    DEFAULT_CONFIG_FILE = str(pathlib.Path().home().joinpath('.wk_platform.toml'))
    USER_CONFIG_FILE = str(pathlib.Path().home().joinpath('.wk_platform.toml'))
elif platform.system() == "Linux":
    DEFAULT_CONFIG_FILE = "/etc/wk_trade_toolkit/wk_platform.toml"
    USER_CONFIG_FILE = str(pathlib.Path().home().joinpath('.wk_platform.toml'))
else:
    raise Exception("Unsupported system:", platform.system())


class Configuration(metaclass=SingletonType):
    """
    保存配置信息
    """

    def __init__(self):
        # self._config_path = config_path
        self._config = copy.deepcopy(DEFAULT_CONFIG)
        self._config_loaded = True
        self._use_system_config = False
        self._use_user_config = False
        try:
            self._config = pydash.objects.merge(self._config, toml.load(DEFAULT_CONFIG_FILE))
            self._use_system_config = True
        except FileNotFoundError:
            pass
        try:
            self._config = pydash.objects.merge(self._config, toml.load(USER_CONFIG_FILE))
            self._use_user_config = True
        except FileNotFoundError:
            pass

    @property
    def data_src(self):
        return self._config['wk_trade_toolkit']['data_src']

    @property
    def use_system_config(self):
        return self._use_system_config

    @property
    def use_user_config(self):
        return self._use_user_config

    @property
    def config_loaded(self):
        return self._config_loaded

    def load_config(self, config_path):
        self._config_path = config_path
        self._config = toml.load(self._config_path)
        self._config_loaded = True

    @property
    def config(self):
        return self._config

    @property
    def bench_dir(self):
        return self._config['store']['bench_dir']

    @property
    def result_dir(self):
        p = pathlib.Path(self._config['store']['result_dir'])
        if not p.exists():
            p.mkdir()
        return p

    @property
    def data_dir(self):

        if DATA_VERSION == "v0.1":
            p = pathlib.Path(self._config['store']['data_dir'])

        else:
            p = pathlib.Path(self._config['store']['data_dir2'])
        if not p.exists():
            p.mkdir()

        if DATA_VERSION != "v0.1":
            p = p.joinpath(DATA_VERSION).joinpath('data')
        return p

    @property
    def risk_factor_dir(self):
        p = pathlib.Path(self._config['store']['data_dir2'])
        p = p.joinpath(DATA_VERSION).joinpath('factor_exposure')
        return p

    def get(self, config_path: Iterable, default_value=None):
        current_obj = self._config
        try:
            for field in config_path:
                current_obj = current_obj[field]
            return current_obj
        except KeyError as e:
            if default_value is None:
                raise e
            else:
                return default_value

    def update_config(self, config_):
        self._config = pydash.objects.merge(self._config, config_)


config = Configuration()
