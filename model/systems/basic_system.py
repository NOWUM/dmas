# third party modules
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem

# model modules
from demandlib.electric_profile import StandardLoadProfile


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
        self._fuel_types = ['solar', 'wind', 'water', 'bio', 'lignite', 'coal', 'gas', 'nuclear']
        self.generation = {fuel: np.zeros(T) for fuel in self._fuel_types + ['total']}
        self.fuel_type = fuel_type
        self.generation_system = dict(maxPower=maxPower)

        # -> Demand Configuration
        self._demand_types = ['power', 'heat']
        self._consumer_types = ['household', 'business', 'industry', 'agriculture']
        self.demand = {demand: np.zeros(T) for demand in self._demand_types}
        self.demand_type = demand_type

        if self.demand_type is not None:
            self.demand_generator = StandardLoadProfile(demandP, type=self.demand_type)
        else:
            self.demand_generator = None

        self._cash_types = ['profit', 'fuel', 'emission', 'start_ups', 'forecast']
        self.cash_flow = {cash: np.zeros(T) for cash in self._cash_types}

        self.power = np.zeros(T)
        self.volume = np.zeros(T)

        self.weather = pd.DataFrame()
        self.prices = pd.DataFrame()
        self.date = pd.Timestamp(2022, 1, 1)

    def _set_total_generation(self):
        for fuel in self._fuel_types:
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

    def _reset_data(self) -> None:
        for fuel in self._fuel_types + ['total']:
            self.generation[fuel] = np.zeros(self.T)
        for demand in self._demand_types:
            self.demand[demand] = np.zeros(self.T)
        for cash in self._cash_types:
            self.cash_flow[cash] = np.zeros(self.T)

        self.volume = np.zeros(self.T)
        self.power = np.zeros(self.T)

