# Introduction
PyTrader is a python based backtesting engine that currently supports ETF based strategies. There is some development work going into adding support for Futures and Currencies. The backtesting engine runs based on a supplied calendar by iterating over available time stamps for trading. By default the engine forces a strategy to not have look ahead bias by only exposing the strategy to data available at time t and trading on t+1. Additionally, transaction costs can be set and adjusted by the user.

# Example
A simple moving average strategy on SPY can be found in examples/etfs/ma.py. The strategy is configurable by YAML files for fast iteration and comparing results from multiple tests.

The benchmark for strategies is configurable but by default these are the outputs:


```Loading universe: TEST
Running backtest...
Starting value $25,000
Ending value $87,415
Ran in 1 seconds

----- Statistics -----
strategy                 Strategy 0
Cumulative Ret    249.7% ($ 62,415)
Annualized Ret        5.4% ($ 248)%
Annualized Vol        12.2% ($ 361)
Sharpe Ratio    (%) 0.44 | ($) 0.69
Max Drawdown     -18.9% ($ -13,036)
MAR             (%) 0.29 | ($) 0.02
Sortino         (%) 0.48 | ($) 0.70
Rolling Sharpe                 0.46
Beta                           0.38
Alpha                         2.89%
Rolling Alpha                -2.80%
Rolling Beta                   0.62
```

![SampleStratPerformance](https://user-images.githubusercontent.com/61852120/147672350-e9291d5d-1ef5-4451-a5a6-0b7cf491b903.png)
![SampleStratRisk](https://user-images.githubusercontent.com/61852120/147672362-ce905329-37e9-491f-b308-d48e62304600.png)
