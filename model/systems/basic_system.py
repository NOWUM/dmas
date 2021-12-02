# third party modules
import numpy as np
import pandas as pd
from datetime import date


class EnergySystem:

    def __init__(self, T=24):
        self.T = T
        self.t = np.arange(T)

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

        self.date = date(2018,1,1)

        self.weather = None
        self.prices = None

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices

    def optimize(self):
        pass
