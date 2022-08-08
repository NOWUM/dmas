# third party modules
import numpy as np
import pandas as pd
from tqdm import tqdm
import multiprocessing as mp
import logging
import time


# model modules
from systems.demand_pv_bat import PvBatModel
from systems.demand_pv import HouseholdPvModel
from systems.demand import HouseholdModel, BusinessModel, IndustryModel, AgricultureModel
from aggregation.basic_portfolio import PortfolioModel


log = logging.getLogger('demand_portfolio')
log.setLevel('INFO')


def optimize_energy_system(item):
    item.optimize()
    return item


class DemandPortfolio(PortfolioModel):

    def __init__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)
        self.worker = mp.Pool(4)

    def __del__(self):
        self.worker.close()

    def add_energy_system(self, energy_system):

        if energy_system['type'] == 'battery':
            model=PvBatModel(T=self.T, **energy_system)
            self.capacities['solar'] += energy_system['maxPower'] # [kW]
        elif energy_system['type'] == 'solar':
            model=HouseholdPvModel(T=self.T, **energy_system)
            self.capacities['solar'] += energy_system['maxPower'] # [kW]
        elif energy_system['type'] == 'household':
            model=HouseholdModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'business':
            model = BusinessModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'industry':
            model=IndustryModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'agriculture':
            model=AgricultureModel(self.T, **energy_system)

        self.energy_systems.append(model)

    def get_order_book(self, name, power=None):
        power = power or self.power
        order_book = {}
        for t in self.t:
            if power[t] < 0:
                order_book[t] = dict(type='demand',
                                     hour=t,
                                     block_id=t,
                                     name=name,
                                     price=3, # €/kWh
                                     volume=power[t])

        df = pd.DataFrame.from_dict(order_book, orient='index')
        if df.empty:
            raise Exception(f'no orders found; order_book: {order_book}')
        return df.set_index(['block_id', 'hour', 'name'])


    def optimize(self, date, weather, prices):
        """
        optimize the portfolio for the day ahead market
        :return: time series in [kW]
        """
        start_time = time.time()
        self.set_parameter(date, weather, prices)
        for model in tqdm(self.energy_systems):
            model.set_parameter(date=self.date, weather=self.weather.copy(), prices=self.prices.copy())
        log.info(f'set parameter in {time.time() - start_time:.2f} seconds')

        try:
            self.reset_data()  # -> rest time series data
            self.energy_systems = self.worker.map(optimize_energy_system, tqdm(self.energy_systems))
            log.info(f'optimized portfolio')
        except Exception as e:
            log.error(f'error in portfolio optimization: {repr(e)}')

        try:
            for model in tqdm(self.energy_systems):
                for key, value in model.generation.items():
                    self.generation[key] += value # [kW]
                for key, value in model.demand.items():
                    self.demand[key] += value # [kW]

            for key, value in self.generation.items():
                if key != 'total':
                    self.generation['total'] += value # [kW]

            self.power = self.generation['total'] - self.demand['power']

        except Exception as e:
            log.error(f'error in collecting result: {repr(e)}')

        return self.power



