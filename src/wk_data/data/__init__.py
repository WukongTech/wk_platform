# import pkg_resources

# 吸收合并重组列表
# change_dt 为以新股票进行交易的前一个交易日
# stock_conversion_file = pkg_resources.resource_filename(__name__, 'stock_conversion.csv')

import importlib.resources
from contextlib import ExitStack
import atexit


file_manager = ExitStack()
atexit.register(file_manager.close)
ref = importlib.resources.files('wk_data.data') / 'stock_conversion.csv'
stock_conversion_file = file_manager.enter_context(importlib.resources.as_file(ref))