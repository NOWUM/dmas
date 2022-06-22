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

    def __init__(self, T=24, date='2020-01-01', steps=np.array([-10, -5, 0, 5, 100])/1e3):
        super().__init__(T, date)
        self.steps = steps

    def add_energy_system(self, energy_system):
        model = PowerPlant(T=self.T, steps=self.steps, **energy_system)
        self.capacities[str(energy_system['fuel']).replace('_combined', '')] += energy_system['maxPower'] # [kW]
        self.energy_systems.append(model)

    def set_parameter(self, date, weather, prices):
        super().set_parameter(date, weather, prices)

        for model in tqdm(self.energy_systems):
            model.set_parameter(self.date, self.weather.copy(), self.prices.copy())
            model.build_model()

    def build_model(self, get_linked_result):
        # query the DayAhead results
        for model in self.energy_systems:
            power = np.zeros(24)
            committed_power = get_linked_result(model.name)
            for index, row in committed_power.iterrows():
                power[int(row.hour)] = float(row.volume)

            model.committed_power = power
            model.build_model()

    def optimize(self):
        """
        optimize the portfolio for the day ahead market
        :return: time series in [kW]
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
                    self.generation[str(model.power_plant['fuel']).replace('_combined', '')] += value
                for key, value in model.demand.items():
                    self.demand[key] += value
                for key, value in model.cash_flow.items():
                    self.cash_flow[key] += value
            for key, value in self.generation.items():
                if key != 'total':
                    self.generation['total'] += value

            self.power = self.generation['total'] - self.demand['power']

        except Exception as e:
            log.error(f'error in collecting result: {repr(e)}')

        return self.power
