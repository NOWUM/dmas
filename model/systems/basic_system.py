# third party modules
import numpy as np
import pandas as pd
from datetime import date


class EnergySystem:

    def __init__(self, T=24):
        self.T = T
        self.demand = dict(power=np.zeros((self.T,)),     # elect. power
                           heat=np.zeros((self.T,)))      # heat demand

        self.generation = dict(total=np.zeros((self.T,), dtype=float),       # total generation
                               solar=np.zeros((self.T,), dtype=float),       # solar generation
                               wind=np.zeros((self.T,), dtype=float),        # wind generation
                               water=np.zeros((self.T,), dtype=float),       # run river or storage generation
                               bio=np.zeros((self.T,), dtype=float),         # biomass generation
                               lignite=np.zeros((self.T,), dtype=float),     # lignite generation
                               coal=np.zeros((self.T,), dtype=float),        # hard coal generation
                               gas=np.zeros((self.T,), dtype=float),         # gas generation
                               nuclear=np.zeros((self.T,), dtype=float))         # nuclear generation

        self.power = np.zeros((self.T,))

        self.date = date(2018,1,1)

        self.weather = None
        self.prices = None

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices

    def optimize(self):
        pass