# third party modules
import numpy as np
import pandas as pd
from gurobipy import *


class PortfolioModel:

    def __init__(self, T=24, dt=1, gurobi=False, date='2020-01-01'):

        self.date = pd.to_datetime(date)                    # current day
        self.energySystems = {}                             # energy systems in portfolio

        # load reference profiles
        # heat
        self.Ref_Temperature = np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32)
        self.Ref_factors = np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32)
        # power
        self.Ref_H0 = np.asarray(np.load(open(r'./data/Ref_H0.array', 'rb')), np.float32)
        self.Ref_G0 = np.asarray(np.load(open(r'./data/Ref_G0.array', 'rb')), np.float32)
        self.Ref_Rlm = np.asarray(np.load(open(r'./data/Ref_RLM.array', 'rb')), np.float32)

        # calculation and optimization parameters
        self.T = T                                          # number of steps
        self.t = np.arange(T)                               # array with steps
        self.dt = dt                                        # step length [h]

        self.weather = {}                                   # weather data (forecast)
        self.prices = {}                                    # price (forecast)

        # sum parameters
        self.power = np.zeros(T, dtype=np.float)            # sum power         [MW]
        self.emission = np.zeros(T, dtype=np.float)         # sum emissions     [€]
        self.fuel = np.zeros(T, dtype=np.float)             # sum fuel          [€]
        self.volume = np.zeros(T, dtype=np.float)           # total volume      [MWh]
        self.start = np.zeros(T, dtype=np.float)            # total start costs [€]

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

        # initialize milp optimization for power plant dispatch
        if gurobi:
            self.m = Model('aggregation')
            self.m.Params.OutputFlag = 0
            self.m.Params.TimeLimit = 30
            self.m.Params.MIPGap = 0.05
            self.m.__len__ = 1

    # set parameter for optimization
    def set_parameter(self, date, weather, prices):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices

    # ddd energy system to portfolio
    def add_energy_system(self, name, energysystem):
        pass

    # build model with constrains for optimization
    def build_model(self, response=[]):
        pass

    def optimize(self):
        power = np.zeros_like(self.t)
        return power

    def fix_planing(self):
        power = np.zeros_like(self.t)
        return power


if __name__ == "__main__":
    test = PortfolioModel()
    pass


