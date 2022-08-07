# third party modules
import numpy as np
import pandas as pd
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

    def __init__(self, T=24, date='2020-01-01', agentname='', price=0.0):
        super().__init__(T, date)
        self.lock_generation = True
        self.worker = mp.Pool(4)
        self.agentname = agentname
        self.res_price = price

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

        self.capacities[energy_system['type']] += energy_system['maxPower'] # [kW]

        self.energy_systems.append(model)

    def set_total_generation(self):
        fuels = [*self.generation.keys()]
        fuels.remove('total')
        self.generation['total'] = np.zeros((self.T,), float)
        for fuel in fuels:
            self.generation['total'] += self.generation[fuel]

    def get_order_book(self, power=None):
        power = power or self.power
        order_book = {}
        for t in np.arange(len(power)):
            order_book[t] = dict(type='generation',
                                    hour=t,
                                    block_id=t,
                                    order_id=0,
                                    name=self.agentname,
                                    price=self.res_price,
                                    volume=power[t])
        df = pd.DataFrame.from_dict(order_book, orient='index')
        if df.empty:
            df = pd.DataFrame(columns=['type', 'block_id', 'hour', 'order_id', 'name', 'price', 'volume'])
        df = df.set_index(['block_id', 'hour', 'order_id', 'name'])

        return df

    def optimize(self, date, weather, prices):
        """
        optimize the portfolio for the day ahead market
        :return: time series in [kW]
        """

        self.set_parameter(date, weather, prices)
        for model in self.energy_systems:
            model.set_parameter(date=self.date, weather=self.weather.copy(), prices=self.prices.copy())

        try:
            self.reset_data()  # -> rest time series data
            self.energy_systems = self.worker.map(optimize_energy_system, tqdm(self.energy_systems))
            log.info(f'optimized portfolio')
        except Exception as e:
            log.error(f'error in portfolio optimization: {repr(e)}')

        try:
            for model in tqdm(self.energy_systems):
                for key, value in model.generation.items():
                    self.generation[key] += value # [kW]
                for key, value in model.demand.items():
                    self.demand[key] += value # [kW]
                for key, value in model.cash_flow.items():
                    self.cash_flow[key] += value

            for key, value in self.generation.items():
                if key != 'total':
                    self.generation['total'] += value

            self.power = self.generation['total'] - self.demand['power']

        except Exception as e:
            log.error(f'error in collecting result: {repr(e)}')

        return self.power

    def optimize_post_market(self, committed_power):
        power = self.generation['total'] - self.demand['power']
        priority_fuel = ['wind', 'bio', 'water', 'solar']
        to_reduce = power - committed_power
        log.info(f'need to reduce res by {to_reduce}')
        for t in self.t:
            for fuel in priority_fuel:
                if to_reduce[t] > 1e-9:
                    # substract delta from generation in priority order
                    # if first generation is not enough, reduce completely
                    # and reduce second generation too
                    if self.generation[fuel][t] - to_reduce[t] < 0:
                        to_reduce[t] -= self.generation[fuel][t]
                        self.generation[fuel][t] = 0
                        log.info(f'reduced {fuel} - {t} still need to reduce {to_reduce[t]}')
                    else:
                        self.generation[fuel][t] -= to_reduce[t]
                        to_reduce[t] = 0
                        log.info(f'reduced {fuel} - {t}')
        self.set_total_generation()
        self.power = self.generation['total']

        return self.power

if __name__ == '__main__':
    rpf = RenewablePortfolio()
    bm = {
        'type': 'bio',
        'maxPower': 300,
    }
    rpf.add_energy_system(bm)
    assert rpf.capacities['bio']==300
    power = rpf.optimize('2018-01-01', {}, {})
    assert (power == 300).all()
    assert (rpf.generation['total'] == 300).all()
    assert (rpf.generation['bio'] == 300).all()
    final_power = rpf.optimize_post_market(power)
    assert (final_power == 300).all()

    # second day
    power = rpf.optimize('2018-01-02', {}, {})
    power[2:4] = 0
    final_power = rpf.optimize_post_market(power)
    assert((power == final_power).all())
