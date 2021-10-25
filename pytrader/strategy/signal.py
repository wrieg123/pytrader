import numpy as np

class DeadAsset:
    def __init__(self, id):
        self.identifier = id


class Signal:

    def __init__(self, indicator, granularity = 'daily', assets = None, grouping = None, name = None):
        self.indicator = indicator
        self.granularity = granularity
        self.assets = assets
        self.grouping = grouping
        self.name = self.indicator.__class__.__name__ if name is None else name

        self.granularity_tradeable = f'{granularity}_tradeable'

    def __getitem__(self, val):
        return self.indicator[val]

    def refresh(self):
        if self.assets is None or all(getattr(a, self.granularity_tradeable) for a in self.assets):
            self.indicator.refresh()

class SignalGroup:

    def __init__(self, run_all = False):
        self.run_all = run_all
        self.signals =  {}
        self.asset_map = {}
    
    def add(self, signal, asset_based = False):
        """
        adds the included signal to the group and may map in to an asset for selective running
        """
        if not asset_based:
            id_tuple = None
            signal_name = signal.name
        else:
            identifiers = [a.identifier for a in signal.assets]
            id_tuple = tuple(identifiers)
            identifiers.append(signal.name)
            signal_name = tuple(identifiers)
        
        if id_tuple is not None:
            for asset_id in id_tuple:
                if asset_id not in self.asset_map:
                    self.asset_map[asset_id] = []
                self.asset_map[asset_id].append(signal_name)
        self.signals[signal_name] = signal
    

    def _get_signals_by_asset(self, assets):
        signals = []
        for asset in assets:
            signals.extend(self.asset_map.get(asset, []))
        return list(set(signals))
    

    def parse_getter_args(self, assets, names):
        if assets is not None:
            signal_names = self._get_signals_by_asset(self, assets)
        elif names is not None:
            signal_names = names
        else:
            signal_names = list(self.signals)
        return signal_names


    def get_signal_tree(self, assets = None, names=None):
        """
        return asset_name : grouping : signal name : np.array for signals
        """
        signal_names = self.parse_getter_args(assets, names)
        tree = {} 

        for name in signal_names:
            signal = self.signals[name]
            assets = [DeadAsset('NA')] if signal.assets is None else signal.assets

            for asset in assets:
                if asset.identifier not in tree:
                    tree[asset.identifier] = {}
                if signal.grouping not in tree[asset.identifier]:
                    tree[asset.identifier][signal.grouping] = {}
                tree[asset.identifier][signal.grouping][signal.name] = signal[:]
        return tree
            
    def get_signal_array(self, assets = None, names=None):
        """
        returns asset_name : np_colstack array of signals
        """
        signal_names = parse_getter_args(assets, names)
        
        tree = {}
        for name in signal_names:
            signal = self.signals[name]
            assets = [DeadAsset('NA')] if signal.assets is None else signal.assets
            
            for asset in assets:
                if asset not in tree:
                    tree[asset.identifier] = []
                tree[asset.identifier].append(signal[:])
        return {a : np.column_stack(v) for a, v in list(tree.items())}
    
    def get_signal_tree_by_names(self, names):
        """
        simple getter
        """
        return {name : self.signals[name][:] for name in names}

    def refresh(self, assets = None):
        if assets is None or self.run_all:
            for i in self.signals.values():
                i.refresh()
        else:
            for signal in self._get_signals_by_asset(assets):
                self.signals[signal].refresh()
