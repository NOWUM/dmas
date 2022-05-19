# third party modules
import numpy as np
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
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'solar':
            model=HouseholdPvModel(T=self.T, **energy_system)
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'household':
            model=HouseholdModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'business':
            model = BusinessModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'industry':
            model=IndustryModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'agriculture':
            model=AgricultureModel(self.T, **energy_system)

        self.energy_systems.append(model)

    def build_model(self, response=None):
        for model in tqdm(self.energy_systems):
            model.set_parameter(date=self.date, weather=self.weather.copy(), prices=self.prices.copy())

    def optimize(self):
        self.reset_data()

        t = time.time()
        self.energy_systems = self.worker.map(optimize_energy_system, tqdm(self.energy_systems))
        log.info(f'optimize took {np.round(time.time() - t,2)}')

        t = time.time()
        for model in tqdm(self.energy_systems):
            for key, value in model.generation.items():
                self.generation[key] += value
            for key, value in model.demand.items():
                self.demand[key] += value

        self.power = self.generation['total'] - self.demand['power']
        log.info(f'append took {np.round(time.time() - t,2)}')

        return self.power/10**3  # [MW]



