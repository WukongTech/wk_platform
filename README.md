# 量化回测平台

回测平台基于PyAlgoTrade开发，推荐在Jupyter Notebook中使用

## 用法示例
这里展示了一个使用持仓表完成回测的例子
```python
import pandas as pd
from wk_platform.contrib.strategy import WeightStrategy # 使用根据权重调仓的策略
from wk_platform.config import StrategyConfiguration


config = StrategyConfiguration()  # 实例化配置类

# 读取权重文件
weight_df = pd.read_csv("weightFile/case1.csv")
# 初始化策略
strategy = WeightStrategy(weight_df, '20210101', '20210301', config=config)
# 运行策略
strategy.run()
```

获取result中的各个DataFrame可按以下方法操作
```python
result = strategy.result # 直接查看结果 df
print(result.keys()) # 显示所有可用的指标
result['策略指标'] # 根据名称查看对应的DataFrame
```

如果要将结果保存到excel文件中
```python
strategy.result.to_excel("result/result202201.xlsx")
```
