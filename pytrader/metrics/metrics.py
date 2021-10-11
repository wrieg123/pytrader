from pytrader.feeds.db.etfs import ETFPrices
from pytrader.portfolio import Portfolio

from matplotlib.gridspec import GridSpec
from datetime import datetime

import matplotlib.pyplot as plt
import matplotlib as mpl
import pandas as pd
import numpy as np


class Metrics:

    def __init__(self, strategies, rolling = 63, index = 'SPY', show_as = '%', factor_betas = [], buying_power = 0, start_date = None):
        self.strategies = strategies
        self.rolling = rolling
        self.index = index
        self.show_as = show_as
        self.factor_betas = factor_betas
        self.buying_power = buying_power
        self.start_date = start_date
        
        # init
        self.stats = []
        self.portfolios = []
        if self.index is not None:
            self.index_df = self._gather_index()
            #self._parse_index()
        self._parse_strategies()
        
    def _parse_strategies(self):
        for i, strategy in enumerate(self.strategies):
            df = self._prep_df(self._clean_portfolio(strategy.portfolio))
            stats, df = self._generate_statistics(i, df)
            self.stats.append(stats)
            self.portfolios.append(df)

    def _clean_portfolio(self, portfolio):
        df = pd.DataFrame(
                np.column_stack([portfolio.eod_tstamps, portfolio.eod_values]), 
                columns = ['Timestamp', 'Value']
                )
        df['DateTime'] = df['Timestamp'].apply(lambda x: pd.to_datetime(datetime.fromtimestamp(x).strftime('%Y-%m-%d')))
        df = df.set_index('DateTime').sort_index()[['Value']]
        if self.start_date is not None:
            df = df.loc[df.index >= pd.to_datetime(self.start_date)].copy()
        return df


    def _prep_df(self, df):
        df['date'] = df.index
        df['year'] = df['date'].apply(lambda x: pd.to_datetime(f'{x.year}-01-01'))
        df['year-month'] = df['date'].apply(lambda x: pd.to_datetime(f'{x.year}-{x.month:02d}-01'))

        df['expanding_max'] = df['Value'].expanding().max()

        # drawdown
        df['$dd'] = df['Value'] - df['expanding_max']
        if self.buying_power == 0:
            df['%dd'] = (df['Value'] / df['expanding_max'] - 1).apply(lambda x: 0 if x > 0 else x)
        else:
            df['%dd'] = df['$dd'] / self.buying_power
        
        # change 
        df['$change'] = df['Value'].diff().fillna(0)
        if self.buying_power == 0:
            df['%change'] = df['Value'].pct_change().fillna(0)
        else:
            df['%change'] = df['$change'] / self.buying_power

        # cum returns
        df['$cum'] = df['$change'].cumsum().fillna(0)
        df['%cum'] = (1+df['%change']).cumprod().fillna(1)
        return df


    def _gather_index(self):
        df = ETFPrices(
                tickers = [self.index],
                ).data
        df['date'] = pd.to_datetime(df['trade_date'])
        df = df.pivot_table(index = 'date', columns = 'ticker', values = 'close')
        df = df.sort_index()
        if self.start_date is not None:
            df = df.loc[df.index >= pd.to_datetime(self.start_date)].copy()
        df['index_value'] = df[self.index]
        #df['index_value'] = df['index_value'].fillna(method = 'ffill')
        #df['index_value'] = df['index_value'].fillna(method = 'bfill')
        df['index_%change'] = df['index_value'].pct_change().fillna(0)
        df['index_%cum'] = (1+df['index_%change']).cumprod().fillna(1)

        #starting_value = df['Value'].head(1).values[0]
        #idx_starting_v = df['index_value'].head(1).values[0]
        #shares = starting_value / idx_starting_v
        shares = 1
        df['index_$change'] = (df['index_value'].diff() * shares).fillna(0)
        df['index_$cum'] = df['index_$change'].cumsum().fillna(0)
        df['index_expanding_$max'] = df['index_$cum'].expanding().max()
        df['index_expanding_%max'] = df['index_%cum'].expanding().max()
        df['index_$dd'] = df['index_$cum'] - df['index_expanding_$max']
        df['index_%dd'] = (df['index_%cum'] / df['index_expanding_%max'] - 1).apply(lambda x: 0 if x > 0 else x)

        if self.show_as == '%':
            df['rolling_index_performance'] = (1+df[f'index_{self.show_as}change']).rolling(self.rolling).agg(lambda x: x.prod()) - 1
        else:
            df['rolling_index_performance'] = df[f'index_{self.show_as}change'].rolling(self.rolling).sum()


        return df

    def _generate_statistics(self, strat_num, df):
        stats = {'strategy': f'Strategy {strat_num}'}
        
        if self.show_as == '%':
            df['rolling_sharpe'] = ((1+df[f'{self.show_as}change']).rolling(self.rolling).agg(lambda x: x.prod()) - 1) / (df[f'{self.show_as}change'].std() * np.sqrt(self.rolling))
        else:
            df['rolling_sharpe'] = df[f'{self.show_as}change'].rolling(self.rolling).sum() / (df[f'{self.show_as}change'].std() * np.sqrt(self.rolling))

        df['rolling_sharpe_avg'] = df['rolling_sharpe'].mean()

        if self.index is not None:
            df['index_values_mapped'] = self.index_df['index_value']
            df['index_values_mapped'] = df['index_values_mapped'].fillna(method = 'ffill')
            df['index_values_mapped'] = df['index_values_mapped'].fillna(method = 'bfill')

            starting_value = df['Value'].head(1).values[0]
            idx_starting_v = df['index_values_mapped'].head(1).values[0]
            shares = idx_starting_v / starting_value
            df['index_$change'] = df['index_values_mapped'].diff().fillna(0) * shares
            df['index_%change'] = df['index_values_mapped'].pct_change().fillna(0)
            if self.show_as == '%':
                df['rolling_index_performance'] = (1+df[f'index_{self.show_as}change']).rolling(self.rolling).agg(lambda x: x.prod()) - 1
            else:
                df['rolling_index_performance'] = df[f'index_{self.show_as}change'].rolling(self.rolling).sum()

            cov_cols = [f'{self.show_as}change', f'index_{self.show_as}change']
            df['rolling_beta'] = df[cov_cols].rolling(self.rolling).cov().unstack()[cov_cols[0]][cov_cols[1]] / df[f'index_{self.show_as}change'].rolling(self.rolling).var().fillna(1)
            df['rolling_beta_avg'] = df['rolling_beta'].mean()

            if self.show_as == '%':
                df['rolling_performance'] = (1+df[f'{self.show_as}change']).rolling(self.rolling).agg(lambda x: x.prod()) - 1
            else:
                df['rolling_performance'] = df[f'{self.show_as}change'].rolling(self.rolling).sum()
            
            df['rolling_alpha'] = (df['rolling_performance'] - df['rolling_beta'] * df['rolling_index_performance'])
            df['rolling_alpha_avg'] = df['rolling_alpha'].mean()

        stats['cum_ret%'] = df['%cum'].tail(1).values[0] - 1
        stats['cum_ret$'] = df['$cum'].tail(1).values[0]
        stats['ann_ret%'] = ((1+stats['cum_ret%']) ** (252/len(df.index))) - 1
        stats['ann_ret$'] = (df['$cum'].tail(1).values[0] - df['$cum'].head(1).values[0]) / 252 # average yearly return
        stats['ann_vol%'] = df['%change'].std() * np.sqrt(252)
        stats['ann_vol$'] = df['$change'].std()# * np.sqrt(252)
        stats['sharpe%']  = stats['ann_ret%'] / stats['ann_vol%']
        stats['sharpe$']  = stats['ann_ret$'] / stats['ann_vol$']
        stats['maxdd%'] = df['%dd'].min()
        stats['maxdd$'] = df['$dd'].min()
        stats['maxdd%_date'] = df['%dd'].argmin()
        stats['maxdd$_date'] = df['$dd'].argmin()
        stats['mar%'] = stats['ann_ret%'] / abs(stats['maxdd%'])
        stats['mar$'] = stats['ann_ret$'] / abs(stats['maxdd$'])
        stats['sortino%'] = stats['ann_ret%'] / (df.loc[df['%change'] < 0, '%change'].std() * np.sqrt(252))
        stats['sortino$'] = stats['ann_ret$'] / (df.loc[df['$change'] < 0, '$change'].std())
        stats['average_rolling_sharpe'] = df['rolling_sharpe_avg'].tail(1).values[0]

        if self.index is not None:
            stats['average_rolling_beta'] = df['rolling_beta_avg'].tail(1).values[0]
            stats['average_rolling_alpha'] = df['rolling_alpha_avg'].tail(1).values[0]
            stats['beta%'] = df[['%change','index_%change']].cov().unstack()['%change']['index_%change'] / (df['index_%change'].std() ** 2)
            stats['alpha%'] = stats['ann_ret%'] - stats['beta%'] * (((1+df['index_%change']).prod() ** (252 / len(df.index))) - 1)

        return stats, df

    def print(self):
        cleaned_stats_list = []
        for stats in self.stats:
            clean_stats = {'strategy': stats['strategy']}
            clean_stats['Cumulative Ret'] = f"{stats['cum_ret%']*100:.1f}% ($ {stats['cum_ret$']:,.0f})"
            clean_stats['Annualized Ret'] = f"{stats['ann_ret%']*100:.1f}% ($ {stats['ann_ret$']:,.0f})%"
            clean_stats['Annualized Vol'] = f"{stats['ann_vol%']*100:.1f}% ($ {stats['ann_vol$']:,.0f})"
            clean_stats['Sharpe Ratio']   = f"(%) {stats['sharpe%']:.2f} | ($) {stats['sharpe$']:.2f}"
            clean_stats['Max Drawdown']   = f"{stats['maxdd%']*100:.1f}% ($ {stats['maxdd$']:,.0f})" 
            clean_stats['MAR']            = f"(%) {stats['mar%']:.2f} | ($) {stats['mar$']:.2f}"
            clean_stats['Sortino']        = f"(%) {stats['sortino%']:.2f} | ($) {stats['sortino$']:.2f}"

            clean_stats['Rolling Sharpe'] = f"{stats['average_rolling_sharpe']:.2f}"
            if self.index is not None:
                alpha_str = f"{stats['average_rolling_alpha']*100:.2f}%" if self.show_as == '%' else f"${stats['average_rolling_alpha']:,.0f}"
                clean_stats['Beta']          = f"{stats['beta%']:.2f}"
                clean_stats['Alpha']         = f"{stats['alpha%']*100:.2f}%"
                clean_stats['Rolling Alpha'] = alpha_str
                clean_stats['Rolling Beta']  = f"{stats['average_rolling_beta']:.2f}"
            cleaned_stats_list.append(clean_stats)
        df = pd.DataFrame(cleaned_stats_list).set_index('strategy')
        #self.formatted_df = df
        print('\n----- Statistics -----')
        print(df.T)
        print()

    def save(self):
        pass 

    def plot(self):

        color_map = ['blue', 'green', 'red', 'yellow', 'orange', 'purple']

        # PERFORMANCE
        p_fig, p_ax = plt.subplots(1)
        p_fig.suptitle('Strategy Performance') 
        
        show_p_ax = '$1 invested' if self.show_as == '%' else self.show_as
        p_ax.set_ylabel(f"PNL ({show_p_ax})")
        p_ax.yaxis.set_major_formatter(
                mpl.ticker.FuncFormatter(
                    lambda x, pos: f'${x:.2f}' if self.show_as == '%' else f'${x:,.0f}'
                    )
                )
        p_ax_col_name = '%cum' if self.show_as == '%' else '$cum'
        if self.index is not None:
            p_ax.plot(self.index_df.index.values, self.index_df[f'index_{p_ax_col_name}'].values, color='black', label = f'Index ({self.index})')
        for i, returns in enumerate(self.portfolios):
            color = color_map[i]
            p_ax.plot(returns.index.values, returns[p_ax_col_name], color=color, label = f'Strategy {i}')

        # DRAWDOWN, SHARPE, ALPHA, BETA
        o_fig, o_ax = plt.subplots(4)
        o_fig.suptitle('Strategy Risk')
        
        ## DRAWDOWN
        o_ax[0].set_ylabel(f"DD ({self.show_as})")
        o_ax[0].yaxis.set_major_formatter(
                mpl.ticker.FuncFormatter(
                    lambda x, pos: f'{x*100:.1f}%' if self.show_as == '%' else f'${x:,.0f}'
                    )
                )
        o_ax0_col_name = f'{self.show_as}dd'
        if self.index is not None:
            o_ax[0].fill_between(self.index_df.index.values, self.index_df[f'index_{o_ax0_col_name}'].values, color = 'black', alpha = .5, label = f'Index ({self.index})')
        for i, returns in enumerate(self.portfolios):
            ret_idx = [x.strftime('%Y-%m-%d') for x in list(returns.index)]
            idx_idx = [x.strftime('%Y-%m-%d') for x in list(self.index_df.index)]
            color = color_map[i]
            o_ax[0].fill_between(returns.index.values, returns[o_ax0_col_name], color=color, alpha = 0.5, label = f'Strategy {i}')

        ## SHARPE
        o_ax[1].set_ylabel(f"Rolling Sharpe ({self.rolling} days)")
        o_ax[1].yaxis.set_major_formatter(
                mpl.ticker.FuncFormatter(lambda x, pos: f'{x:.2f}')
                )
        for i, returns in enumerate(self.portfolios):
            color = color_map[i]
            o_ax[1].plot(returns.index.values, returns['rolling_sharpe'].values, color = color, label = f'Strategy {i}')
            o_ax[1].plot(returns.index.values, returns['rolling_sharpe_avg'].values, '--', color = color)

        if self.index is not None:
            ## ALPHA
            o_ax[2].set_ylabel(f'Rolling Alpha ({self.rolling} days)')
            o_ax[2].yaxis.set_major_formatter(
                    mpl.ticker.FuncFormatter(lambda x, pos: f'{x*100:.1f}%' if self.show_as == '%' else f'${x:,.0f}')
                    )
            for i, returns in enumerate(self.portfolios):
                color = color_map[i]
                o_ax[2].plot(returns.index.values, returns['rolling_alpha'].values, color = color, label = f'Strategy {i}')
                o_ax[2].plot(returns.index.values, returns['rolling_alpha_avg'].values, '--', color = color)
            ## BETA 
            o_ax[3].set_ylabel(f'Rolling Beta ({self.rolling} days)')
            o_ax[3].yaxis.set_major_formatter(
                    mpl.ticker.FuncFormatter(lambda x, pos: f'{x:.3f}')
                    )
            for i, returns in enumerate(self.portfolios):
                color = color_map[i]
                o_ax[3].plot(returns.index.values, returns['rolling_beta'].values, color = color, label = f'Strategy {i}')
                o_ax[3].plot(returns.index.values, returns['rolling_beta_avg'].values, '--', color = color)

        p_ax.legend() 
        o_ax[0].legend()
        o_ax[1].legend()
        o_ax[2].legend()
        o_ax[3].legend()
        plt.show()

