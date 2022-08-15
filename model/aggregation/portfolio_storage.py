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

