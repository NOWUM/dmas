# third party modules
import numpy as np
import pandas as pd
import logging

# model modules
from systems.storage_hydroPlant import Storage
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('storage_portfolio')
log.setLevel('INFO')


class StrPort(PortfolioModel):

    def __int__(self, T: int = 24, date: pd.Timestamp = pd.Timestamp(2022, 1, 1),
                steps=(-10/1e3, -5/1e3, 0, 5/1e3, 100/1e3, 1e6)):
        super().__init__(T=T, date=date, steps=steps)

    def add_energy_system(self, energy_system):
        model = Storage(T=self.T, **energy_system)
        self.capacities['storage'] += energy_system['VMax']
        self.energy_systems.append(model)

    def get_exclusive_orders(self) -> pd.DataFrame:
        total_order_book = [system.get_exclusive_orders().reset_index() for system in self.energy_systems]

        df = pd.concat(total_order_book, axis=0)
        df.set_index(['block_id', 'hour', 'name'], inplace=True)

        if not df.loc[df.isna().any(axis=1)].empty:
            log.error('Orderbook has NaN values')
            log.error(df[df.isna()])

        return df

    def optimize_post_market(self, committed_power, power_prices):
        """
        optimize the portfolio after receiving market results
        :return: time series in [kW] of actual generation
        """

        def get_committed_power(m):
            p = np.zeros(24)
            filtered_cp = committed_power[committed_power['name'] == m.name]
            if not filtered_cp.empty:
                for index, row in filtered_cp.iterrows():
                    p[int(row.hour)] = float(row.volume)
            return p

        for model in self.energy_systems:
            model.optimize_post_market(get_committed_power(model), power_prices)

        allocation = committed_power.groupby('hour').sum().fillna(0)

        alloc = np.zeros(24)
        if not allocation.empty:
            for index, row in allocation.iterrows():
                alloc[int(row.name)] = float(row.volume)

        self.generation['allocation'] = alloc

        self._reset_data()

        for model in self.energy_systems:
            for key, value in model.generation.items():
                self.generation[key] += value           # [kW]
            for key, value in model.demand.items():
                self.demand[key] += value               # [kW]
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value            # [ct]

        self.cash_flow['forecast'] = self.prices['power'].values[:self.T]

        self.power = self.generation['total'] - self.demand['power']

        return self.power