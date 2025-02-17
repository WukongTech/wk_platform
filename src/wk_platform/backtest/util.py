import pandas as pd


def add_net_value_base(bench_df):
    """
    向净值DataFrame增加值为1的首行
    """
    dummy_start: pd.DataFrame = bench_df.head(1).copy()
    dummy_start['date'] = ['00000101']
    dummy_start.set_index('date', inplace=True)
    dummy_start.iloc[0, :] = 1
    new_bench = pd.concat([dummy_start, bench_df])
    return new_bench


def remove_net_value_base(bench_df):
    """
    移除净值的首行
    """
    if bench_df.index[0] == '00000101':
        new_bench = bench_df.drop(index='00000101')
    else:
        new_bench = bench_df
    return new_bench
