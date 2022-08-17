# third party modules
import pandas as pd
import numpy as np

# model modules
from systems.basic_system import EnergySystem
from systems.prosumer import Prosumer
from systems.generation_wind import WindModel
from aggregation.basic_portfolio import PortfolioModel


class RenewablePortfolio(PortfolioModel):

    def __init__(self, T: int = 24, date: pd.Timestamp = pd.Timestamp(2022, 1, 1), name: str = 'ResPort'):
        super().__init__(T=T, date=date, name=name)
        self.priority_fuel = ['wind', 'bio', 'water', 'solar']

    def add_energy_system(self, energy_system: dict) -> None:
        energy_system['fuel_type'] = energy_system['type']
        if energy_system['type'] == 'wind':
            model = WindModel(self.T, energy_system['turbines'])
        elif energy_system['type'] == 'solar':
            model = Prosumer(self.T, demandP=0, **energy_system)
        else:
            model = EnergySystem(self.T, **energy_system)

        self.capacities[energy_system['type']] += energy_system['maxPower']
        self.energy_systems += [model]

    def optimize_post_market(self, committed_power) -> np.array:
        """
        adjust power generation after day ahead clearing
        :return: time series in [kW]
        """
        super().optimize_post_market(committed_power)
        to_reduce = self.power - committed_power
        for t in self.t:
            for fuel in self.priority_fuel:
                if to_reduce[t] > 1e-9:
                    for system in self.energy_systems:
                        if system.fuel_type == fuel:
                            if system.generation[fuel][t] - to_reduce[t] < 0:
                                # print(f'reduced {fuel} to zero')
                                to_reduce[t] -= system.generation[fuel][t]
                                system.generation[fuel][t] = 0
                                system.power[t] = 0
                                system.generation['total'][t] = 0
                            else:
                                # print(f'reduced {fuel} by {to_reduce[t]}')
                                system.generation[fuel][t] -= to_reduce[t]
                                system.power[t] = system.generation[fuel][t]
                                system.generation['total'][t] = system.generation[fuel][t]
                                to_reduce[t] = 0

        self._reset_data()

        self.generation['allocation'] = committed_power
        self.cash_flow['forecast'] = self.prices['power'].values[:self.T]

        for model in self.energy_systems:
            for key, value in model.generation.items():
                self.generation[key] += value           # [kW]
            for key, value in model.demand.items():
                self.demand[key] += value               # [kW]
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value            # [ct]

        self.cash_flow['forecast'] = self.prices['power'].values[:self.T]

        self.power = self.generation['total'] - self.demand['power']

        return self.power


if __name__ == '__main__':
    portfolio = RenewablePortfolio(name='MRK')
    portfolio.add_energy_system(dict(type='bio', maxPower=300))
    portfolio.add_energy_system(dict(type='water', maxPower=500))
    portfolio.optimize(date=pd.Timestamp(2022, 1, 1), prices=pd.DataFrame(), weather=pd.DataFrame())
    assert all(portfolio.power == 800)
    ask_orders = portfolio.get_ask_orders(price=0)
    assert len(ask_orders.index == 24)
    pw = np.asarray([450 for _ in range(24)])
    portfolio.optimize_post_market(pw)
    assert all(portfolio.generation['total'] == 450)
    assert all(portfolio.generation['bio'] == 0)
    assert all(portfolio.generation['water'] == 450)
