from .signal import Signal, SignalGroup
import yaml

class Strategy:

    def __init__(self, config_path = None, indicator_mapping = {}):
        self.config_path = config_path
        self.indicator_mapping = indicator_mapping
        if config_path is not None:
            self.strategy_config = yaml.load(open(config_path,'r'), Loader=yaml.CLoader)
        self.indicator_mapping = indicator_mapping
        self.universes = None
        self.calendar = None
        self.oms = None
        self.portfolio = None
        self.daily_asset_indicators = SignalGroup() # indicators only run daily
        self.asset_indicators = SignalGroup() # indicators that are run intraday
        self.daily_indicators = SignalGroup(run_all = True) # indicators only run daily
        self.indicators = SignalGroup(run_all = True) # indicators that are run intraday
    
    def add_indicator(self, 
            indicator, 
            assets = None, 
            name = None, 
            grouping = None, 
            granularity = 'daily'
            ):
        asset_based = assets is not None
        if asset_based:
            if granularity == 'daily':
                holder = self.daily_asset_indicators
            else:
                holder = self.asset_indicators
        else:
            if granularity == 'daily':
                holder = self.daily_indicators
            else:
                holder = self.indicators
        holder.add(
                Signal(indicator, assets = assets, granularity = granularity, grouping = grouping ,name = name),
                asset_based = asset_based
                )

    def connect(self, universes, portfolio, calendar, oms):
        self.universes=  universes
        self.portfolio = portfolio
        self.calendar = calendar
        self.oms = oms

    def initialize(self):
        if self.config_path is None:
            raise NotImplementedError("You must implmenet an initialize method if no config yaml file is passed in")
        
        self._parse_strategy_config()

    def trade(self):
        raise NotImplementedError("You must implement a trade method that places orders on the OMS")

    def _parse_strategy_config(self):
        for indicator in self.strategy_config.get('indicators',[]):
            self._parse_indicator(indicator)
        
        for k,v in self.strategy_config.get('strategy',{}).items():
            setattr(self, k, v)

    def _parse_indicator(self, indicator_config : dict):
        INDICATOR = self.indicator_mapping.get(indicator_config['indicator'])

        # OPTIONAL
        name = indicator_config.get('name')
        grouping = indicator_config.get('grouping')
        args = indicator_config.get('args', [])
        kwargs = indicator_config.get('kwargs', {})
        granularity = indicator_config.get('granularity', 'daily')
        
        universes = indicator_config.get('universes')

        if universes is not None: 
            ## PARSE BASED ON PRICE SERIES
            for universe_config in universes:
                universe = self.universes[universe_config['universe']]
                for identifier, asset in getattr(universe, universe_config['apply_to']).items():
                    clean_args = []
                    assets = []
                    for arg in args:
                        if arg == 'stream':
                            assets.append(asset)
                            clean_args.append(getattr(asset, universe_config['stream']+'_stream'))
                        elif arg == 'index_stream':
                            idx_asset = self.universes[indicator_config['index_universe']][indicator_config['index_asset']]
                            assets.append(idx_asset)
                            clean_args.append(getattr(idx_asset, universe_config['stream']+'_stream'))
                     
                    self.add_indicator(
                            INDICATOR(*clean_args, **kwargs),
                            assets = tuple(assets),
                            name = name,
                            grouping = grouping,
                            granularity = granularity
                            )
        else:
            clean_args = []
            for arg in args:
                if arg == 'calendar':
                    clean_args.append(self.calendar)
            self.add_indicator(
                    INDICATOR(*clean_args, **kwargs),
                    name = name,
                    grouping = grouping,
                    granularity = granularity
                    )
       
