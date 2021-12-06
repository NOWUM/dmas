# third party modules
import numpy as np
import pandas as pd


class PortfolioModel:

    def __init__(self, T=24, date='2020-01-01'):

        self.date = pd.to_datetime(date)                        # current day
        self.energy_systems = []                                # energy systems in portfolio

        # calculation and optimization parameters
        self.T = T                                              # number of steps
        self.dt = T/24                                          # step length [h]

        self.weather = {}                                       # weather data (forecast)
        self.prices = {}                                        # price (forecast)

        # sum parameters
        self.power = np.zeros(T, dtype=np.float)                # sum power         [MW]
        # self.emission = np.zeros(T, dtype=np.float)           # sum emissions     [€]
        # self.fuel = np.zeros(T, dtype=np.float)               # sum fuel          [€]
        # self.volume = np.zeros(T, dtype=np.float)             # total volume      [MWh]
        # self.start = np.zeros(T, dtype=np.float)              # total start costs [€]

        # installed capacities [MW]
        self.capacities = dict(bio=0., coal=0., gas=0., lignite=0., nuclear=0.,
                               solar=0., water=0., wind=0., storage=0.)

        # current generation series [MW]
        self.generation = dict(total=np.zeros((self.T,), dtype=float),            # total generation
                               solar=np.zeros((self.T,), dtype=float),            # solar generation
                               wind=np.zeros((self.T,), dtype=float),             # wind generation
                               water=np.zeros((self.T,), dtype=float),            # run river or storage generation
                               bio=np.zeros((self.T,), dtype=float),              # biomass generation
                               lignite=np.zeros((self.T,), dtype=float),          # lignite generation
                               coal=np.zeros((self.T,), dtype=float),             # hard coal generation
                               gas=np.zeros((self.T,), dtype=float),              # gas generation
                               nuclear=np.zeros((self.T,), dtype=float))          # nuclear generation

        # current demand series [MW]
        self.demand = dict(power=np.zeros((self.T,), dtype=float),                # total power demand
                           heat=np.zeros((self.T,), dtype=float))                 # total heat demand

    # set parameter for optimization
    def set_parameter(self, date, weather, prices):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices

    # ddd energy system to portfolio
    def add_energy_system(self, energy_system):
        pass

    # build model with constrains for optimization
    def build_model(self, response=None):
        pass

    def optimize(self):
        power = np.zeros((self.T,))
        return power


if __name__ == "__main__":
    test = PortfolioModel()
    pass