from wk_platform.contrib.strategy import WeightStrategy, WeightStrategyConfiguration
from wk_platform.config import MaxUpDownType




begin_date = '20200101'
end_date = '20201231'

config = WeightStrategyConfiguration(
    datasets=('A_SHARE_MARKET', ),
    initial_cash=1e8,
    volume_limit=None,
    commission=0.000,
    stamp_tax=0,
    max_up_down_limit=MaxUpDownType.RELAX_OPEN,
    price_type='open'
)

strategy = WeightStrategy("../../wk_trade_exp/~data/f_corr_ret_turn_post_20_subset.csv",
                          begin_date, end_date, config=config)
strategy.run()
print(strategy.result['回测配置'])
print(strategy.result['策略指标'])
print(strategy.result['调仓日指标'])





