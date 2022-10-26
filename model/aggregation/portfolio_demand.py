# Third party modules
import pandas as pd
import numpy as np


# model modules
from systems.prosumer import Prosumer
from systems.basic_system import EnergySystem, CONSUMER_TYPES
from aggregation.basic_portfolio import PortfolioModel


class DemandPortfolio(PortfolioModel):

    def __init__(self, T: int = 24, date: pd.Timestamp = pd.to_datetime('2020-01-01'), name: str = 'DemPort'):
        super().__init__(T=T, date=date, name=name)
        self._unique_pv_systems = {}
        self._unique_bat_systems = {}
        self._number_of_systems = {}

    def add_energy_system(self, energy_system: dict) -> None:

        if energy_system['type'] in CONSUMER_TYPES:
            model = EnergySystem(demand_type=energy_system['type'], **energy_system)
            self.energy_systems.append(model)
        else:

            model = Prosumer(T=self.T, **energy_system)

            if energy_system['type'] == 'battery':
                key = (energy_system['maxPower'], energy_system['batPower'], energy_system['VMax'],
                       energy_system['azimuth'], energy_system['tilt'], energy_system['demandP'])
                if key not in self._unique_bat_systems.keys():
                    self._unique_bat_systems[key] = dict(num=1, model=model, key=model.name)
                else:
                    self._unique_bat_systems[key]['num'] += 1
            else:
                key = (energy_system['maxPower'], energy_system['azimuth'], energy_system['tilt'],
                       energy_system['demandP'])
                if key not in self._unique_bat_systems.keys():
                    self._unique_pv_systems[key] = dict(num=1, model=model, key=model.name)
                else:
                    self._unique_pv_systems[key]['num'] += 1

            self.capacities['solar'] += energy_system['maxPower']

    def add_unique_systems(self):
        for values in self._unique_pv_systems.values():
            self.energy_systems.append(values['model'])
            self._number_of_systems[values['model'].name] = values['num']
        for values in self._unique_bat_systems.values():
            self._number_of_systems[values['model'].name] = values['num']

    def optimize(self, date: pd.Timestamp, weather: pd.DataFrame, prices: pd.DataFrame) -> np.array:
        super().optimize(date=date, weather=weather, prices=prices)
        self._reset_data()
        for model in self.energy_systems:
            number = 1 if model.fuel_type is None else self._number_of_systems[model.name]
            for key, value in model.generation.items():
                self.generation[key] += value * number          # [kW]
            for key, value in model.demand.items():
                self.demand[key] += value * number              # [kW]
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value * number           # [ct]

        self.power = self.generation['total'] - self.demand['power']

        return self.power


if __name__ == "__main__":
    portfolio = DemandPortfolio()
    portfolio.add_energy_system(dict(type='household', demandP=3500))
    portfolio.optimize(date=pd.Timestamp(2022, 1, 1), prices=pd.DataFrame(), weather=pd.DataFrame())
    order_book = portfolio.get_bid_orders()




