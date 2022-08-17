# third party modules
import logging

import numpy as np
import pandas as pd
from systems.basic_system import (CASH_TYPES, DEMAND_TYPES, FUEL_TYPES,
                                  EnergySystem)
from multiprocessing import Pool
from tqdm import tqdm

def optimize_energy_system(data):
    item, date, weather, prices = data
    item.optimize(date, weather, prices)
    return item


class PortfolioModel:

    def __init__(self, T: int = 24, date: pd.Timestamp = pd.to_datetime('2020-01-01'),
                 steps: tuple = (0,), name: str = None):
        """
        Represents a portfolio of EnergySystems.
        Its capacities, generation and demand is in MW
        """
        self.logger = logging.getLogger(f'portfolio_{name}')
        self.name = name

        self.T, self.t, self.dt = T, np.arange(T), 1

        # -> Generation Configuration
        self.generation = {fuel: np.zeros(T) for fuel in FUEL_TYPES + ['total']}
        self.capacities = {fuel: 0 for fuel in FUEL_TYPES}

        # -> Demand Configuration
        self.demand = {demand: np.zeros(T) for demand in DEMAND_TYPES}

        self.cash_flow = {cash: np.zeros(T) for cash in CASH_TYPES}

        self.power = np.zeros(T)
        self.volume = np.zeros(T)

        self.weather = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.date = date

        self.energy_systems: list[EnergySystem] = []
        self.steps = steps

        self.pool = Pool(4)

        self.weather = pd.DataFrame()
        self.prices = pd.DataFrame()

    def __del__(self):
        self.pool.close()

    def _set_parameter(self, date: pd.Timestamp, weather: pd.DataFrame, prices: pd.DataFrame) -> None:
        self.date = date
        self.weather = weather
        self.prices = prices

    def add_energy_system(self, energy_system) -> None:
        """
        adds an energy system to the portfolio
        - power values of the EnergySystem are in kW
        - the capacities of the Portfolio is stored in MW
        """
        pass

    def get_bid_orders(self, price: float = 3) -> pd.DataFrame:
        order_book = {t: dict(type='demand', hour=t, block_id=t, name=self.name, price=price,
                              volume=-max(self.demand['power'][t] - self.generation['total'][t], 0)) for t in self.t}
        df = pd.DataFrame.from_dict(order_book, orient='index')
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def get_ask_orders(self, price: float = -0.5) -> pd.DataFrame:
        order_book = {t: dict(type='generation', hour=t, block_id=t, name=self.name, price=price,
                              volume=self.generation['total'][t] - self.demand['power'][t]) for t in self.t}
        df = pd.DataFrame.from_dict(order_book, orient='index')
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def optimize(self, date: pd.Timestamp, weather: pd.DataFrame, prices: pd.DataFrame) -> np.array:
        """
        optimize the portfolio for the day ahead market
        :return: time series in [kW]
        """
        self._reset_data()
        self._set_parameter(date, weather, prices)

        params = []
        date, weather, prices = date, weather.copy(), prices.copy()
        for system in self.energy_systems:
            params.append((system, date, weather, prices))
        self.energy_systems = self.pool.map(optimize_energy_system, tqdm(params))

        for model in self.energy_systems:
            for key, value in model.generation.items():
                self.generation[key] += value           # [kW]
            for key, value in model.demand.items():
                self.demand[key] += value               # [kW]
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value            # [ct]

        self.power = self.generation['total'] - self.demand['power']

        return self.power

    def _reset_data(self) -> None:
        for fuel in FUEL_TYPES + ['total']:
            self.generation[fuel] = np.zeros(self.T)
        for demand in DEMAND_TYPES:
            self.demand[demand] = np.zeros(self.T)
        for cash in CASH_TYPES:
            self.cash_flow[cash] = np.zeros(self.T)

        self.volume = np.zeros(self.T)
        self.power = np.zeros(self.T)

    def optimize_post_market(self, committed_power) -> np.array:
        if self.prices.empty:
            raise Exception('Optimize Post Market without Prices - agent started mid simulation?')