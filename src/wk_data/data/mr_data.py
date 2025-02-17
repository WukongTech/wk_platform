from dataclasses import dataclass

import pandas as pd

from wk_data.data import stock_conversion_file
from wk_data.mappings import register_get_no_arg


@dataclass()
class MRRecord:
    change_dt: str
    new_windcode: str
    ratio: float


@register_get_no_arg('mr_data')
def get_merger_reorganization_data(original=False):
    """
    获取并购重组后的代码、股价变动信息
    注意：已经剔除退市相关记录
    """
    mapping = {
        "CHANGE_DATE": "change_dt",
        "OLD_TICKER": "old_windcode",
        "NEW_TICKER": "new_windcode",
        "OLD_CLOSE": "old_clode",
        "NEW_CLOSE": "new_close",
        "RATIO": "ratio"
    }
    data = pd.read_csv(stock_conversion_file).rename(columns=mapping)
    data = data[data['new_windcode'].notna()]
    if original:
        return data

    mr_map = {}
    for t in data.itertuples():
        mr_map[t.old_windcode] = MRRecord(str(t.change_dt), t.new_windcode, t.ratio)
    return mr_map