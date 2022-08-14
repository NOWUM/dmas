# Third party modules
import pandas as pd

# model modules
from systems.prosumer import Prosumer
from systems.basic_system import EnergySystem, CONSUMER_TYPES
from aggregation.basic_portfolio import PortfolioModel


class DemandPortfolio(PortfolioModel):

    def __init__(self, T: int = 24, date: pd.Timestamp = pd.to_datetime('2020-01-01'), name: str = 'DemPort'):
        super().__init__(T=T, date=date, name=name)

    def add_energy_system(self, energy_system: dict) -> None:

        if energy_system['type'] in CONSUMER_TYPES:
            model = EnergySystem(demand_type=energy_system['type'], **energy_system)
        else:
            model = Prosumer(T=self.T, **energy_system)
            self.capacities['solar'] += energy_system['maxPower']

        self.energy_systems.append(model)


if __name__ == "__main__":
    portfolio = DemandPortfolio()
    portfolio.add_energy_system(dict(type='household', demandP=3500))
    portfolio.optimize(date=pd.Timestamp(2022, 1, 1), prices=pd.DataFrame(), weather=pd.DataFrame())
    order_book = portfolio.get_bid_orders()




