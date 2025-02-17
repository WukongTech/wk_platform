from __future__ import annotations

import pathlib
import datetime
import pytz
import pandas as pd
from collections import OrderedDict
import zipfile


class BackTestResult:
    def __init__(self):
        """
        """
        self.__results = OrderedDict()
        self.__plot_func = {}

    def add_metric(self, key: str, data: pd.DataFrame | OrderedDict, *, plot_func=None, formatter=lambda x: x):
        """
        Parameters
        --------
        key : str
            指标名称
        data : DataFrame | OrderedDict
            保存结果的DateFrame，或若干个存储在OrderDict中的相同结构DataFrame
        plot_func: Callable
            绘图函数
        formatter: Callable
            格式化输出的后处理函数
        """
        try:
            df = self.__results[key]
            raise ValueError(f"`{key}` existed")
        except KeyError:
            self.__results[key] = formatter(data)
            if plot_func:
                self.__plot_func[key] = plot_func

    def __getitem__(self, k):
        return self.__results[k]

    def items(self):
        return self.__results.items()

    def keys(self):
        return self.__results.keys()

    def to_excel(self, file_path):
        with pd.ExcelWriter(file_path) as writer:
            for k, v in self.__results.items():
                if isinstance(v, pd.DataFrame):
                    v.to_excel(writer, k)
                elif isinstance(v, OrderedDict):
                    count = 0
                    for name, df in v.items():
                        df.to_excel(writer, k, startrow=count, header=True, index=True)
                        count += df.shape[0] + 1
        # writer.save()

    def plot(self, key: str, **kwargs):
        plot_func = self.__plot_func[key]
        return plot_func(self.__results[key], **kwargs).render_notebook()


class BackTestResultSet:
    def __init__(self):
        self.__results = {}

    def __getitem__(self, k):
        return self.__results[k]

    def __setitem__(self, key, value: BackTestResult):
        self.__results[key] = value

    def items(self):
        return self.__results.items()

    def keys(self):
        return self.__results.keys()

    def to_excel(self, directory, time_tag=False, compress=False):
        if not isinstance(directory, pathlib.Path):
            directory = pathlib.Path(directory)
        assert directory.is_dir()
        files = []
        for k, v in self.__results.items():
            tag = '_result'
            if time_tag:
                tag = tag + '_' + datetime.datetime.now(pytz.timezone('PRC')).strftime("%Y-%m-%d_%H%M%S")
            file_name = directory.joinpath(k + tag + '.xlsx').absolute()
            v.to_excel(file_name)
            files.append(file_name)
        if compress:
            name = 'backtest_result_' + datetime.datetime.now(pytz.timezone('PRC')).strftime("%Y-%m-%d_%H%M%S") + '.zip'
            name = directory.joinpath(name).absolute()
            with zipfile.ZipFile(name, 'w') as f:
                for file_name in files:
                    f.write(file_name, file_name.name)



