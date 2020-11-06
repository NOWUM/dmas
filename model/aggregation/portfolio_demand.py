# third party modules
import numpy as np


# model modules
from components.consumer_pvBat import PvBatModel
from components.consumer_pv import PvModel
from components.consumers import H0Model, G0Model, RlmModel
from aggregation.portfolio import PortfolioModel


class DemandPortfolio(PortfolioModel):

    def __int__(self, T=24, dt=1, gurobi=False, date='2020-01-01'):
        super().__init__(T, dt, gurobi, date)

    def add_energy_system(self, name, energy_system):

        data = energy_system[name]

        # build photovoltaic with battery
        if data['typ'] == 'PvBat':
            data.update(dict(model=PvBatModel(lat=data['position'][0],
                                              lon=data['position'][1],
                                              photovoltaic=data['PV'],
                                              battery=data['Bat'],
                                              e_el=data['demandP'])))
        # build photovoltaic
        elif data['typ'] == 'Pv':
            data.update(dict(model=PvModel(lat=data['position'][0],
                                           lon=data['position'][1],
                                           photovoltaic=data['PV'],
                                           e_el=data['demandP'])))
        # build residential
        elif data['typ'] == 'H0':
            data.update(dict(model=H0Model(e_el=data['demandP'])))
        # build Trade and Service
        elif data['typ'] == 'G0':
            data.update(dict(model=G0Model(e_el=data['demandP'])))
        # build industry
        elif data['typ'] == 'RLM':
            data.update(dict(model=RlmModel(e_el=data['demandP'])))

        self.energy_systems.update(energy_system)

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

