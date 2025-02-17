from dataclasses import dataclass

import pandas as pd


def dataclass_list_to_dataframe(data, cls: dataclass = None, exclude=None) -> pd.DataFrame:
    """
    将列表形式的dataclass对象转换为DataFrame
    """
    if cls is None:
        assert len(data) > 0
        cls = data[0].__class__
    if exclude is None:
        exclude = []
    fields = [k for k in cls.__dataclass_fields__.keys() if k not in exclude]
    data_dict = {}
    for name in fields:
        data_dict[name] = []

    for entry in data:
        for name in fields:
            value = entry.__getattribute__(name)
            data_dict[name].append(value)

    df = pd.DataFrame(data_dict)
    return df
