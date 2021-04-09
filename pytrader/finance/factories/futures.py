from pytrader.feeds.static import FuturesTS
from pytrader.utils import split_to_chunks
from .worker import Worker, WorkerGroup


class FuturesFactory(WorkerGroup):

    def __init__(
            self, 
            identifiers = [],
            bar_frequency = 'daily',
            start_date = None,
            end_date = None,
            start_time = None,
            end_time = None,
            ):
        super().__init__(identifiers)
        self.bar_frequency = bar_frequency
        self.start_date = start_date
        self.end_date = end_date
        self.start_time = start_time
        self.end_time = end_time
        self.not_tradeable = []
        self.__update_group()
        
    def __update_group(self):
        if len(self.identifiers) > 0:
            print('Initializing feed with',len(self.identifiers), 'identifiers')
            if len(self.identifiers) >= 50:
                manager = Manager()
                holder = manager.list()

                pool = Pool(processes = 4)
                for group in split_to_chunks(self.identifiers, 50):
                    pool.apply_async(self.add_group, args = [group, holder])
                pool.close()
                pool.join()
            else:
                holder = []
                self.add_group(self.identifiers, holder)
                master_df = pd.concat(holder)

            available = list(master_df['contract'].unique())
            tradeable = set(self.identifiers).intersection(set(available))
            self.not_tradeable = list(set(available).symmetric_difference(set(self.identifiers)))

            grouped = dict(tuple(master_df.groupby('contract')))
            print('Not Tradeable:', self.not_tradeable)
            for contract in tradeable:
                self.group[contract] = Worker(
                                            contract, 
                                            feed = grouped[contract].pivot_table(
                                                                        index = 'index', 
                                                                        columns = 'field',
                                                                        values = 'value'
                                                                        ).to_dict(orient = 'index')
                                            )
   

    def add_group(self, group, holder):
        print('Adding in group:',group[0],'->', group[-1])
        if len(group) > 0:
            #TODO: this shouldn't be a naked try
            #try:
            df = FuturesTS(
                        contracts,
                        frequency = self.bar_frequency,
                        start_date = self.start_date,
                        end_date = self.end_date,
                        start_time = self.start_time,
                        end_time = self.end_time,
                        ).unstack().dropna().reset_index()
            #except:
            #    df = None

            if df is None:  
                for contract in group:
                    self.not_tradeable.append(contract)
                    print(contract, 'not tradeable')
                return

            df.columns = ['contract', 'field', 'index', 'value']

            holder.append(df)


    def set_streams(self, streams):
        for k, s in list(streams.items()):
            if k not in self.not_tradeable:
                self.group[k].set_stream(s)
