# third party modules
import numpy as np
from tqdm import tqdm
import time
import logging

# model modules
from systems.storage_hydroPlant import Storage
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('storage_portfolio')
log.setLevel('INFO')

class StrPort(PortfolioModel):

    def __int__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)


    def add_energy_system(self, energy_system):
        model = Storage(T=self.T, **energy_system)
        self.capacities['storages'] += energy_system['VMax']/1e3 # [kW] -> [MW]
        self.energy_systems.append(model)

    def build_model(self, response=None):
        for model in tqdm(self.energy_systems):
            model.set_parameter(date=self.date, weather=self.weather.copy(), prices=self.prices.copy())

    def optimize(self):

        self.reset_data()

        t = time.time()
        for model in self.energy_systems:
            model.optimize()
        log.info(f'optimize took {np.round(time.time() - t, 2)}')

        t = time.time()
        for model in tqdm(self.energy_systems):
            for key, value in model.generation.items():
                self.generation['total'] += value/1e3 # [kW] -> [MW]
                self.generation['water'] += value/1e3 # [kW] -> [MW]
            for key, value in model.demand.items():
                self.demand[key] += value/1e3 # [kW] -> [MW]
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value

        self.power = self.generation['total'] - self.demand['power']
        log.info(f'append took {np.round(time.time() - t,2)}')

        return self.power