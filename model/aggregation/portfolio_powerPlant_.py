# third party modules
import numpy as np
from tqdm import tqdm
import multiprocessing as mp
import logging
import time

# model modules
from systems.generation_powerPlant_ import PowerPlant
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('power_plant_portfolio')
log.setLevel('INFO')


def optimize_energy_system(item):
    item.optimize()
    return item


class PowerPlantPortfolio(PortfolioModel):

    def __init__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)
        self.lock_generation = True
        self.worker = mp.Pool(4)

    def add_energy_system(self, energy_system):
        model = PowerPlant(T=self.T, steps=[-10, -5, 0, 5, 100], **energy_system)
        self.capacities[str(energy_system['fuel']).replace('_combined', '')] += energy_system['maxPower']
        self.energy_systems.append(model)

    def build_model(self, response=None):
        for model in tqdm(self.energy_systems):
            model.set_parameter(date=self.date, weather=self.weather.copy(), prices=self.prices.copy())

    def optimize(self):
        self.reset_data()

        t = time.time()
        for model in self.energy_systems:
            model.optimize()
        # self.energy_systems = self.worker.map(optimize_energy_system, tqdm(self.energy_systems))
        log.info(f'optimize took {np.round(time.time() - t, 2)}')

        t = time.time()
        for model in tqdm(self.energy_systems):
            for key, value in model.generation.items():
                self.generation[str(model.power_plant['fuel']).replace('_combined', '')] += value
            for key, value in model.demand.items():
                self.demand[key] += value
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value

        self.power = self.generation['total'] - self.demand['power']
        log.info(f'append took {np.round(time.time() - t,2)}')

        return self.power
