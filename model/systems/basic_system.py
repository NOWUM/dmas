# third party modules
import numpy as np
import pandas as pd

# model modules
from demandlib.electric_profile import StandardLoadProfile
from pvlib.pvsystem import PVSystem

FUEL_TYPES = ['solar', 'wind', 'water', 'bio',
              'lignite', 'coal', 'gas', 'nuclear']
DEMAND_TYPES = ['power', 'heat']
CONSUMER_TYPES = ['household', 'business', 'industry', 'agriculture']
CASH_TYPES = ['profit', 'fuel', 'emission', 'start_ups', 'forecast']


def get_solar_generation(generation_system: PVSystem, weather: pd.DataFrame) -> np.array:
    r = generation_system.get_irradiance(solar_zenith=weather['zenith'], solar_azimuth=weather['azimuth'],
                                         dni=weather['dni'], ghi=weather['ghi'], dhi=weather['dhi'])
    power = r['poa_global'] * generation_system.arrays[0].module_parameters['pdc0'] / 1e3
    return np.asarray(power).flatten()


class EnergySystem:

    def __init__(self, T: int = 24, maxPower: float = 0, fuel_type: str = None,
                 demandP: float = 0, demand_type: str = None, *args, **kwargs):
        """
        Describes a basic EnergySystem which behaves dependent from weather and prices.
        It has generation, demand and power in kW.
        """

        self.name = None

        self.T, self.t, self.dt = T, np.arange(T), 1

        # -> Generation Configuration
        self.generation = {fuel: np.zeros(T) for fuel in FUEL_TYPES + ['total']}
        self.fuel_type = fuel_type
        self.generation_system = dict(maxPower=maxPower)

        # -> Demand Configuration
        self.demand = {demand: np.zeros(T) for demand in DEMAND_TYPES}
        self.demand_type = demand_type

        if self.demand_type is not None:
            self.demand_generator = StandardLoadProfile(demandP, type=self.demand_type)
        else:
            self.demand_generator = None

        self.cash_flow = {cash: np.zeros(T) for cash in CASH_TYPES}

        self.power = np.zeros(T)
        self.volume = np.zeros(T)

        self.weather = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.date = pd.Timestamp(2022, 1, 1)

    def _set_total_generation(self):
        for fuel in FUEL_TYPES:
            self.generation['total'] += self.generation[fuel]

    def _set_parameter(self, date: pd.Timestamp, weather: pd.DataFrame = None, prices: pd.DataFrame = None):
        self.date = date
        self.weather = weather
        self.prices = prices

    def _get_demand(self, d_type: str = 'power'):
        return self.demand[d_type]

    def _get_generation(self):
        return self.generation['total']

    def optimize(self, date: pd.Timestamp, weather: pd.DataFrame = None, prices: pd.DataFrame = None, steps=None):
        self._reset_data()
        self._set_parameter(date, weather, prices)
        if self.fuel_type is not None:
            power = np.ones(self.T) * self.generation_system['maxPower']
            self.generation[self.fuel_type] = power.flatten()
            self._set_total_generation()
        if self.demand_type is not None:
            power = self.demand_generator.run_model(self.date)
            self.demand['power'] = power.flatten()

        self.power = self.generation['total'] - self.demand['power']

        return self.power

    def optimize_post_market(self, committed_power: np.array, power_prices: np.array = None):
        pass

    def get_bid_orders(self, price: float = 3) -> pd.DataFrame:
        order_book = {t: dict(type='demand', hour=t, block_id=t, name=self.name, price=price,
                              volume=self.demand['power'][t] - self.generation['total'][t]) for t in self.t}
        df = pd.DataFrame.from_dict(order_book, orient='index')
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def get_ask_orders(self, price: float = -0.5) -> pd.DataFrame:
        order_book = {t: dict(type='generation', hour=t, block_id=t, name=self.name, price=price,
                              volume=self.generation['total'][t] - self.demand['power'][t]) for t in self.t}
        df = pd.DataFrame.from_dict(order_book, orient='index')
        df = df.set_index(['block_id', 'hour', 'name'])
        return df

    def _reset_data(self) -> None:
        for fuel in FUEL_TYPES + ['total']:
            self.generation[fuel] = np.zeros(self.T)
        for demand in DEMAND_TYPES:
            self.demand[demand] = np.zeros(self.T)
        for cash in CASH_TYPES:
            self.cash_flow[cash] = np.zeros(self.T)

        self.volume = np.zeros(self.T)
        self.power = np.zeros(self.T)

