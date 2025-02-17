
# from functools import lru_cache
# from wk_data.wind_data.stock_daily_status import (
#     get_a_share_daily_status,
#     get_trade_calendar
# )
# from wk_data.wind_data.stock_indicator import (
#     prepare_a_share_eod_derivative_indicator_date_source,
#     fetch_a_share_eod_derivative_indicator
# )
#
# from wk_data.wind_data.index_industry import get_index_industry_weight
# from wk_data.data_source import (
#     get_merger_reorganization_data
# )
#
# from wk_data.wind_data.index_daily_status import get_index_daily_status
#
# from wk_data.wind_data.index_component import get_index_industry_components_weight


# DISPATCHER_MAPPING = {
#     "a_share_market": get_a_share_daily_status,
#     "index_market": get_index_daily_status,
#     "a_share_indicator": fetch_a_share_eod_derivative_indicator,
#     "trade_calendar": get_trade_calendar
#
# }
#
# DISPATCHER_MAPPING_NO_ARG = {
#     "mr_data": get_merger_reorganization_data,
#     "index_industry_weight": get_index_industry_weight,
#     "index_components_weight": get_index_industry_components_weight
# }
#
# UPDATE_DISPATCHER_MAPPING = {
#     "a_share_indicator": lambda: prepare_a_share_eod_derivative_indicator_date_source(fetch=True).update()
# }


# # @lru_cache(maxsize=3)
# def get(name, *, begin_date=None, end_date=None, **kwargs):
#     if begin_date is not None:
#         return DISPATCHER_MAPPING[name](begin_date=begin_date, end_date=end_date, **kwargs)
#     else:
#         return DISPATCHER_MAPPING_NO_ARG[name](**kwargs)
#
#
# def update(name):
#     return UPDATE_DISPATCHER_MAPPING[name]()

import importlib.util
import pathlib

from wk_data.mappings import GET_MAPPING
from wk_data.mappings import GET_MAPPING_NO_ARG
from wk_data.mappings import UPDATE_MAPPING
from wk_data.mappings import SYNC_MAPPING
from wk_data.mappings import RegisterEnv

__all__ = [
    'get', 'update', 'show_mappings', 'sync'
]


def _calc_import_name(file_path: pathlib.Path):
    relative_path = file_path.relative_to(pathlib.Path(__file__).parent).parts
    relative_path = [ part.split('.')[0] for part in relative_path]
    self_import_tuple = __name__.split('.')
    import_name_tuple = self_import_tuple[:-1] + relative_path
    return '.'.join(import_name_tuple)


def _init(module_path: pathlib.Path):
    """
    手动导入数据模块
    """
    root_path = pathlib.Path(module_path)
    with RegisterEnv():
        for f in root_path.iterdir():
            # print(f)
            if f.is_file() and f.suffix == '.py':
                import_name = _calc_import_name(f)
                spec = importlib.util.spec_from_file_location(import_name, f.absolute())
                spec.loader.exec_module(
                    importlib.util.module_from_spec(spec)
                )


def get(name, *, begin_date=None, end_date=None, **kwargs):
    if begin_date is not None:
        return GET_MAPPING[name](begin_date=begin_date, end_date=end_date, **kwargs)
    else:
        return GET_MAPPING_NO_ARG[name](**kwargs)


def format_print_keys(keys):
    for k in keys:
        print('\t', k)


def show_mappings():
    print('GET_MAPPING:')
    format_print_keys(GET_MAPPING.keys())

    print('GET_MAPPING_NO_ARG:')
    format_print_keys(GET_MAPPING_NO_ARG.keys())

    print('UPDATE_MAPPING:')
    format_print_keys(UPDATE_MAPPING.keys())

    print('SYNC_MAPPING:')
    format_print_keys(SYNC_MAPPING.keys())


def update(name, **kwargs):
    return UPDATE_MAPPING[name](**kwargs)


def sync(name, **kwargs):
    return SYNC_MAPPING[name](**kwargs)


_init(pathlib.Path(__file__).parent.joinpath('wind_data'))
_init(pathlib.Path(__file__).parent.joinpath('data'))
_init(pathlib.Path(__file__).parent.joinpath('local_data'))
