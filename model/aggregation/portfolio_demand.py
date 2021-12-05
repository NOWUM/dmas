# third party modules
import numpy as np


# model modules
from systems.demand_pv_bat import PvBatModel
from systems.demand_pv import HouseholdPvModel
from systems.demand import HouseholdModel, BusinessModel, IndustryModel
from aggregation.portfolio import PortfolioModel


class DemandPortfolio(PortfolioModel):

    def __int__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)

    def add_energy_system(self, energy_system):

        # build photovoltaic with battery
        if energy_system['type'] == 'bat':
            energy_system.update(dict(model=PvBatModel(T=self.T, **energy_system)))
            self.capacities['solar'] += energy_system['maxPower']
        # build photovoltaic
        elif energy_system['type'] == 'solar':
            energy_system.update(dict(model=HouseholdPvModel(T=self.T, **energy_system)))
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'household':
            energy_system.update(dict(model=HouseholdModel(T=self.T, **energy_system)))
        elif energy_system['type'] == 'business':
            energy_system.update(dict(model=BusinessModel(T=self.T, **energy_system)))
        elif energy_system['type'] == 'industry':
            energy_system.update(dict(model=IndustryModel(T=self.T, **energy_system)))

        self.energy_systems.update({energy_system['unitID']: energy_system})

    def build_model(self, response=[]):
        for _, data in self.energy_systems.items():
            data['model'].set_parameter(weather=self.weather, date=self.date)

    def optimize(self):

        # optimize each energy system
        for _, data in self.energy_systems.items():
            data['model'].optimize()
        # collect results
        power = np.zeros_like(self.t)       # initialize power with zeros
        try:
            power, solar, demand = [], [], []
            for _, value in self.energy_systems.items():
                if 'Pv' in value['typ']:
                    factor = (value['num'] - value['EEG'])
                    power.append(value['model'].power * factor)
                    solar.append(value['model'].generation['powerSolar'] * factor)
                    demand.append(value['model'].demand['power'] * factor)
                else:
                    power.append(value['model'].power)
                    solar.append(value['model'].generation['powerSolar'])
                    demand.append(value['model'].demand['power'])
            power = np.sum(np.asarray(power, np.float), axis=0)
            self.generation['powerSolar'] = np.sum(np.asarray(solar, np.float), axis=0)
            self.demand['power'] = np.sum(np.asarray(demand, np.float), axis=0)

        except Exception as e:
            print(e)

        self.power = power
        self.generation['powerTotal'] = power

        return self.power


if __name__ == "__main__":

    portfolio = DemandPortfolio()

