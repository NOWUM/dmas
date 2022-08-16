# third party modules
import numpy as np
import pandas as pd
import logging

# model modules
from systems.powerPlant import PowerPlant
from aggregation.basic_portfolio import PortfolioModel

log = logging.getLogger('power_plant_portfolio')
log.setLevel('INFO')


class PowerPlantPortfolio(PortfolioModel):

    def __init__(self, T: int = 24, date: pd.Timestamp = pd.Timestamp(2022, 1, 1),
                 steps=(-10/1e3, -5/1e3, 0, 5/1e3, 100/1e3, 1e6)):
        super().__init__(T=T, date=date, steps=steps)

    def add_energy_system(self, energy_system):
        model = PowerPlant(T=self.T, steps=self.steps, **energy_system)
        self.capacities[str(energy_system['fuel']).replace('_combined', '')] += energy_system['maxPower'] # [kW]
        self.energy_systems.append(model)

    def optimize_post_market(self, committed_power, power_prices):
        """
        optimize the portfolio after receiving market results
        :return: time series in [kW] of actual generation
        """
        if self.prices.empty:
            log.error('Optimize Post Market without Prices - agent started mid simulation?')
            return self.power

        def get_committed_power(m):
            p = np.zeros(24)
            filtered_cp = committed_power[committed_power['name'] == m.name]
            if not filtered_cp.empty:
                for index, row in filtered_cp.iterrows():
                    p[int(row.hour)] = float(row.volume)

            return p

        for model in self.energy_systems:
            model.optimize_post_market(get_committed_power(model), power_prices)

        self._reset_data()

        allocation = committed_power.groupby('hour').sum().fillna(0)
        alloc = np.array(pd.DataFrame(index=range(self.T), data=allocation))

        self.generation['allocation'] = np.zeros(self.T) + alloc.flatten()
        self.cash_flow['forecast'] = self.prices['power'].values[:self.T]

        for model in self.energy_systems:
            for key, value in model.generation.items():
                self.generation[key] += value           # [kW]
            for key, value in model.demand.items():
                self.demand[key] += value               # [kW]
            for key, value in model.cash_flow.items():
                self.cash_flow[key] += value            # [ct]

        self.power = self.generation['total'] - self.demand['power']

        return self.power

    def get_ask_orders(self, price: float = -0.5) -> pd.DataFrame:
        if len(self.energy_systems) < 1:
            raise Exception('no systems to get orders from')
        total_order_book = [system.get_ask_orders().reset_index() for system in self.energy_systems]

        df = pd.concat(total_order_book, axis=0)
        df.set_index(['block_id', 'hour', 'name'], inplace=True)

        if not df.loc[df.isna().any(axis=1)].empty:
            log.error('Orderbook has NaN values')
            log.error(df[df.isna()])

        return df


if __name__ == '__main__':
    ppp = PowerPlantPortfolio()
    plant = {'unitID':'x',
            'fuel':'lignite',
            'maxPower': 300, # kW
            'minPower': 100, # kW
            'eta': 0.4, # Wirkungsgrad
            'P0': 120,
            'chi': 0.407/1e3, # t CO2/kWh
            'stopTime': 12, # hours
            'runTime': 6, # hours
            'gradP': 300, # kW/h
            'gradM': 300, # kW/h
            'on': 1, # running since
            'off': 0,
            'startCost': 1e3 # €/Start
            }
    ppp.add_energy_system(plant)

    power_price = [0.0649, 0.0618, 0.0641, 0.064, 0.0644, 0.0597, 0.065, 0.0589, 0.0638, 0.0597, 0.0595, 0.0625, 0.0628, 0.0606, 0.0607, 0.0603, 0.062, 0.0643, 0.0637, 0.0594, 0.0615, 0.0642, 0.06, 0.061, 0.064, 0.0621, 0.0628, 0.0616, 0.0601, 0.0622, 0.0644, 0.0607, 0.0622, 0.0633, 0.0638, 0.065, 0.0615, 0.0635, 0.06, 0.0629, 0.065, 0.0599, 0.0625, 0.0633, 0.062, 0.0617, 0.0631, 0.0619]
    co = np.ones(48) * 23.8  # * np.random.uniform(0.95, 1.05, 48)     # -- Emission Price     [€/t]
    gas = np.ones(48) * 0.03  # * np.random.uniform(0.95, 1.05, 48)    # -- Gas Price          [€/kWh]
    lignite = np.ones(48) * 0.015  # * np.random.uniform(0.95, 1.05)   # -- Lignite Price      [€/kWh]
    coal = np.ones(48) * 0.02  # * np.random.uniform(0.95, 1.05)       # -- Hard Coal Price    [€/kWh]
    nuc = np.ones(48) * 0.01  # * np.random.uniform(0.95, 1.05)        # -- nuclear Price      [€/kWh]

    prices = dict(power=power_price, gas=gas, co=co, lignite=lignite, coal=coal, nuc=nuc)
    prices = pd.DataFrame(data=prices, index=pd.date_range(start='2018-01-01', freq='h', periods=48))

    power = ppp.optimize(pd.Timestamp(2018, 1, 1), pd.DataFrame(), prices)
    o_book = ppp.energy_systems[0].get_ask_orders()
    # from systems.powerPlant import visualize_orderbook
    # visualize_orderbook(o_book)
    # clean_spread = ppp.energy_systems[0].ge()
    # assert clean_spread == 0.0617165
    assert (power[0:22] >= 100).all() # stay on
    assert ~power[22:23].all() # two hours off

    df = pd.DataFrame([power, power_price[:24]]).T
    # df['clean_spread'] = clean_spread
    # print(df)
    comm_power = pd.DataFrame(dict(volume=power))
    comm_power['name'] = 'x'
    comm_power['hour'] = range(len(power))
    ppp.optimize_post_market(comm_power, np.ones_like(power))
    pass
