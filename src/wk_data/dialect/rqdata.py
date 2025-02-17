import pandas as pd

from wk_util.data import symbol_trans, CodePattern


def transform_factor_exposure(data: pd.DataFrame):

    order_book_id = data['order_book_id'].unique()[0]
    windcode = symbol_trans(order_book_id, CodePattern.UPPER_SUFFIX)
    data['windcode'] = windcode

    data_factor = data[[
        'date', 'windcode',
        'residual_volatility', 'momentum', 'earnings_yield', 'comovement', 'growth', 'liquidity', 'size',
        'leverage', 'book_to_price', 'beta', 'non_linear_size'
    ]]

    data_sector = data.drop(columns=[
        'residual_volatility', 'momentum', 'earnings_yield', 'comovement', 'growth', 'liquidity', 'size',
        'leverage', 'book_to_price', 'beta', 'non_linear_size', 'order_book_id', 'windcode'
    ])

    sectors = data_sector.columns.tolist()
    sectors.remove('date')

    data_sector_melted = pd.melt(data_sector, id_vars=['date'], value_vars=sectors)

    if data_sector_melted['value'].sum() == 0:
        return pd.DataFrame()
    data_sector_melted = data_sector_melted[data_sector_melted['value'] != 0]
    # print(data_sector_melted.head())
    # print(data_sector.head())
    # print(data_sector_melted.shape[0], data_sector.shape[0])
    if data_sector_melted.shape[0] != data_sector.shape[0]:
        idx = data_sector.shape[0] - data_sector_melted.shape[0]
        # print(data_sector.loc[idx]['date'], data_sector_melted.iloc[0]['date'])
        assert data_sector.loc[idx]['date'] == data_sector_melted.iloc[0]['date']
    data_sector_melted = data_sector_melted.rename(columns={'variable': 'sector'}).drop(columns=['value'])

    data_factor = data_factor.merge(data_sector_melted, on='date')
    data_factor['date'] = pd.to_datetime(data_factor['date'], format='%Y-%m-%d').map(lambda x: x.strftime('%Y%m%d'))
    return data_factor

