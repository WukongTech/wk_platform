import datetime
import pandas as pd
import wk_db


def update_date_mapping():
    start_date_str = '20100101'
    end_date_str = datetime.datetime.now().strftime("%Y%m%d")
    index_windcode = '000001.SH'

    stmt = f"select distinct trade_dt from AINDEXHS300FREEWEIGHT where trade_dt>='{start_date_str}' and s_info_windcode='{index_windcode}'"
    # stmt = stmt.bindparams(start_date=start_date_str, windcode='000001.SH')
    index_df = wk_db.read_sql(stmt)

    data = pd.DataFrame()
    date_list = pd.date_range(start=start_date_str, end=end_date_str).strftime("%Y%m%d")
    data['date'] = date_list
    data = data.merge(index_df, left_on="date", right_on="trade_dt", how='left')
    data['trade_dt'] = data['trade_dt'].ffill()
    data = data.rename(columns={"trade_dt": "last_index_dt"}).dropna()

    wk_db.to_sql(data, table_name='date_mapping', db_loc='quant_data_sync', index=False, if_exists='replace')