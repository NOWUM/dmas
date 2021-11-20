# third party modules
import numpy as np
import pandas as pd


class EnergySystem:

    def __init__(self, t=np.arange(24), T=24, dt=1):
        self.t, self.T, self.dt = t, T, dt

        self.demand = dict(power=np.zeros_like(self.t),     # elect. power
                           heat=np.zeros_like(self.t))      # heat demand

        self.generation = dict(powerTotal=np.zeros_like(self.t, dtype=float),       # total generation
                               powerSolar=np.zeros_like(self.t, dtype=float),       # solar generation
                               powerWind=np.zeros_like(self.t, dtype=float),        # wind generation
                               powerWater=np.zeros_like(self.t, dtype=float),       # run river or storage generation
                               powerBio=np.zeros_like(self.t, dtype=float),         # biomass generation
                               powerLignite=np.zeros_like(self.t, dtype=float),     # lignite generation
                               powerCoal=np.zeros_like(self.t, dtype=float),        # hard coal generation
                               powerGas=np.zeros_like(self.t, dtype=float),         # gas generation
                               powerNuc=np.zeros_like(self.t, dtype=float))         # nuclear generation

        self.power = np.zeros_like(self.t)

        self.date = pd.to_datetime('2018-01-01')
        self.weather = None
        self.prices = None

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices

    def optimize(self):
        pass
