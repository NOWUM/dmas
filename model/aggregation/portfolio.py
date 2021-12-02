# third party modules
import numpy as np
import pandas as pd


class PortfolioModel:

    def __init__(self, T=24, date='2020-01-01'):

        self.date = pd.to_datetime(date)                        # current day
        self.energy_systems = {}                                # energy systems in portfolio

        # calculation and optimization parameters
        self.T = T                                              # number of steps
        self.t = np.arange(T)                                   # array with steps
        self.dt = T/24                                          # step length [h]

        self.weather = {}                                       # weather data (forecast)
        self.prices = {}                                        # price (forecast)

        # sum parameters
        self.power = np.zeros(T, dtype=np.float)                # sum power         [MW]
        # self.emission = np.zeros(T, dtype=np.float)            # sum emissions     [€]
        # self.fuel = np.zeros(T, dtype=np.float)                # sum fuel          [€]
        # self.volume = np.zeros(T, dtype=np.float)              # total volume      [MWh]
        # self.start = np.zeros(T, dtype=np.float)               # total start costs [€]

        # installed capacities [MW]
        self.capacities = dict(capacityBio=0.,
                               capacityCoal=0.,
                               capacityGas=0.,
                               capacityLignite=0.,
                               capacityNuc=0.,
                               capacitySolar=0.,
                               capacityWater=0.,
                               capacityWind=0.)

        # current generation series [MW]
        self.generation = dict(powerTotal=np.zeros_like(self.t, dtype=float),       # total generation
                               powerSolar=np.zeros_like(self.t, dtype=float),       # solar generation
                               powerWind=np.zeros_like(self.t, dtype=float),        # wind generation
                               powerWater=np.zeros_like(self.t, dtype=float),       # run river or storage generation
                               powerBio=np.zeros_like(self.t, dtype=float),         # biomass generation
                               powerLignite=np.zeros_like(self.t, dtype=float),     # lignite generation
                               powerCoal=np.zeros_like(self.t, dtype=float),        # hard coal generation
                               powerGas=np.zeros_like(self.t, dtype=float),         # gas generation
                               powerNuc=np.zeros_like(self.t, dtype=float))         # nuclear generation

        # current demand series [MW]
        self.demand = dict(power=np.zeros_like(self.t, dtype=float),                # total power demand
                           heat=np.zeros_like(self.t, dtype=float))                 # total heat demand

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
        power = np.zeros_like(self.t)
        return power


if __name__ == "__main__":
    test = PortfolioModel()
    pass


