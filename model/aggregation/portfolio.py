# third party modules
import numpy as np
import pandas as pd


class PortfolioModel:

    def __init__(self, T=24, date='2020-01-01'):

        self.date = pd.to_datetime(date)                        # current day
        self.energy_systems = []                                # energy systems in portfolio

        self.T, self.t, self.dt = T, np.arange(T), 1

        self.weather = {}                                       # weather data (forecast)
        self.prices = {}                                        # price (forecast)

        self.generation, self.demand, self.power, self.capacities = None, None, None, None
        self.reset_data()

    def set_parameter(self, date, weather, prices):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices

    def add_energy_system(self, energy_system):
        pass

    def build_model(self, response=None):
        pass

    def optimize(self):
        power = np.zeros((self.T,))
        return power

    def reset_data(self):
        self.generation = dict(total=np.zeros((self.T,), dtype=float),
                               solar=np.zeros((self.T,), dtype=float),
                               wind=np.zeros((self.T,), dtype=float),
                               water=np.zeros((self.T,), dtype=float),
                               bio=np.zeros((self.T,), dtype=float),
                               lignite=np.zeros((self.T,), dtype=float),
                               coal=np.zeros((self.T,), dtype=float),
                               gas=np.zeros((self.T,), dtype=float),
                               nuclear=np.zeros((self.T,), dtype=float))

        self.demand = dict(power=np.zeros((self.T,), dtype=float),
                           heat=np.zeros((self.T,), dtype=float))

        self.power = np.zeros(self.T, dtype=np.float)

        self.capacities = dict(bio=0.,
                               coal=0.,
                               gas=0.,
                               lignite=0.,
                               nuclear=0.,
                               solar=0.,
                               water=0.,
                               wind=0.,
                               storage=0.)


if __name__ == "__main__":
    test = PortfolioModel()
    pass