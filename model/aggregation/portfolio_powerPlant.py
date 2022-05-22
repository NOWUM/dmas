# third party modules
import numpy as np
from tqdm import tqdm
import logging
import time

# model modules
from systems.generation_powerPlant import PowerPlant
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('power_plant_portfolio')
log.setLevel('INFO')


class PowerPlantPortfolio(PortfolioModel):

    def __init__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)


    def add_energy_system(self, energy_system):
        model = PowerPlant(T=self.T, steps=[-10, -5, 0, 5, 100], **energy_system)
        self.capacities[str(energy_system['fuel']).replace('_combined', '')] += energy_system['maxPower']/1e3 # [kW] -> [MW]
        self.energy_systems.append(model)

    def build_model(self, response=None):
        for model in tqdm(self.energy_systems):
            model.set_parameter(self.date, self.weather.copy(), self.prices.copy())

    def optimize(self):
        """
        optimize the portfolio for the day ahead market
        :return: time series in [MW]
        """
        try:
            self.reset_data()
            for model in self.energy_systems:
                model.optimize()
            log.info(f'optimized portfolio')
        except Exception as e:
            log.error(f'error in portfolio optimization: {repr(e)}')


        try:
            for model in tqdm(self.energy_systems):
                for key, value in model.generation.items():
                    self.generation[str(model.power_plant['fuel']).replace('_combined', '')] += value/1e3 # [kW] -> [MW]
                for key, value in model.demand.items():
                    self.demand[key] += value/1e3 # [kW] -> [MW]
                for key, value in model.cash_flow.items():
                    self.cash_flow[key] += value
            for key, value in self.generation.items():
                if key != 'total':
                    self.generation['total'] += value

            self.power = self.generation['total'] - self.demand['power']

        except Exception as e:
            log.error(f'error in collecting result: {repr(e)}')

        return self.power
