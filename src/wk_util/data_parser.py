from io import StringIO

import pandas as pd


def str2csv(data_str):
    data_io = StringIO(data_str.replace(' ', '').replace('\t', ''),)

    # 使用pandas的read_csv函数读取
    df = pd.read_csv(data_io)
    return df
