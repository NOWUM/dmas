# third party modules
import numpy as np
from tqdm import tqdm
import multiprocessing as mp
import logging
import time

# model modules
from systems.generation_wind import WindModel
from systems.generation_photovoltaic import PvModel
from systems.generation_runRiver import RunRiverModel
from systems.generation_biomass import BioMassModel
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('renewable_portfolio')
log.setLevel('INFO')


def optimize_energy_system(item):
    item.optimize()
    return item


class RenewablePortfolio(PortfolioModel):

    def __init__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)
        self.lock_generation = True
        self.worker = mp.Pool(4)

    def __del__(self):
        self.worker.close()

    def add_energy_system(self, energy_system):

        if energy_system['type'] == 'wind':
            model = WindModel(self.T, energy_system['turbines'])
        if energy_system['type'] == 'solar':
            model = PvModel( self.T, **energy_system)
        if energy_system['type'] == 'water':
            model = RunRiverModel(self.T, **energy_system)
        if energy_system['type'] == 'bio':
            model = BioMassModel(self.T, **energy_system)

        self.capacities[energy_system['type']] += energy_system['maxPower']

        self.energy_systems.append(model)

    def build_model(self, response=None):
        for model in self.energy_systems:
            model.set_parameter(date=self.date, weather=self.weather.copy(), prices=self.prices.copy())

        if response is None:
            self.lock_generation = False
            self.generation['total'] = np.zeros((self.T,))
        else:
            self.lock_generation = True
            self.generation['total'] = np.asarray(response, np.float).reshape((-1,))

    def optimize(self):
        self.reset_data()
        t = time.time()
        self.energy_systems = self.worker.map(optimize_energy_system, tqdm(self.energy_systems))
        log.info(f'optimize took {np.round(time.time() - t, 2)}')

        t = time.time()
        for model in tqdm(self.energy_systems):
            for key, value in model.generation.items():
                self.generation[key] += value
            for key, value in model.demand.items():
                self.demand[key] += value
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value

        self.power = self.generation['total'] - self.demand['power']
        log.info(f'append took {np.round(time.time() - t, 2)}')

        return self.power
        # if self.lock_generation:
        #     power_response = self.generation['total']
        #     for i in self.t:
        #         power_delta = power_response[i] - power[i]
        #         if power_delta < 0:
        #             self.generation['wind'][i] += power_delta
        #             self.generation['wind'][i] = np.max((self.generation['wind'][i], 0))
        #
        # power = self.generation['wind'] + self.generation['solar'] + self.generation['water'] + self.generation['bio']
        #
        # self.generation['total'] = np.asarray(power, np.float).reshape((-1,))
        # self.power = np.asarray(power, np.float).reshape((-1,))



