# third party modules
import numpy as np
from tqdm import tqdm
from pvlib.location import Location
import multiprocessing as mp
import logging
import time

# model modules
from systems.demand_pv_bat import PvBatModel
from systems.demand_pv import HouseholdPvModel
from systems.demand import HouseholdModel, BusinessModel, IndustryModel
from aggregation.portfolio import PortfolioModel
import pandas as pd

log = logging.getLogger('demand_portfolio')
log.setLevel('INFO')

class DemandPortfolio(PortfolioModel):

    def __init__(self, position, T=24, date='2020-01-01'):
        super().__init__(T, date)

        location = Location(longitude=position['lon'], latitude=position['lat'])
        self.solar_positions = location.get_solarposition(pd.date_range(start='1972-01-01 00:00',
                                                                         end='1972-12-31 23:00', freq='h'))

    def add_energy_system(self, energy_system):

        if energy_system['type'] == 'battery':
            model=PvBatModel(T=self.T, **energy_system)
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'solar':
            model=HouseholdPvModel(T=self.T, **energy_system)
            self.capacities['solar'] += energy_system['maxPower']
        elif energy_system['type'] == 'household':
            model=HouseholdModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'business':
            model = BusinessModel(T=self.T, **energy_system)
        elif energy_system['type'] == 'industry':
            model=IndustryModel(T=self.T, **energy_system)

        self.energy_systems.append(model)

    def build_model(self, response=None):
        self.weather['ghi'] = self.weather['dir'] + self.weather['dif']
        self.weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        self.weather.index = pd.date_range(start=self.date, periods=len(self.weather), freq='60min')
        df = self.solar_positions[self.solar_positions.index.day_of_year == self.weather.index[0].day_of_year]
        self.weather['azimuth'] = df['azimuth'].to_numpy()
        self.weather['zenith'] = df['zenith'].to_numpy()

        for model in tqdm(self.energy_systems):
            model.set_parameter(weather=self.weather, date=self.date)

    def f(self, item):
        item.optimize()
        return item

    def optimize(self):

        self.reset_data()

        t = time.time()
        with mp.Pool(4) as p:
            v = p.map(self.f, tqdm(self.energy_systems))
        self.energy_systems = v

        log.info(f'optimize took {time.time() - t}')

        t = time.time()
        power, solar, demand = [], [], []
        for model in self.energy_systems:
            solar.append(model.generation['solar'])
            power.append(model.power)
            demand.append(model.demand['power'])

        self.generation['solar'] = np.sum(np.asarray(solar, np.float), axis=0)
        self.demand['power'] = np.sum(np.asarray(demand, np.float), axis=0)
        self.generation['total'] = self.generation['solar']

        self.power = self.generation['total'] - self.demand['power']
        log.info(f'append took {time.time() - t}')

        return self.power


if __name__ == "__main__":

    portfolio = DemandPortfolio()