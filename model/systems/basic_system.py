# third party modules
import numpy as np
from datetime import date as to_date


class EnergySystem:

    def __init__(self, T: int=24):
        '''
        Describes a basic EnergySystem which behaves dependent from weather and prices.

        It has generation, demand and power in kW.
        '''

        self.date = to_date(2018, 1, 1)

        self.T, self.t, self.dt = T, np.arange(T), 1

        self.weather = {}
        self.prices = {}

        self.generation = None
        self.demand = None
        self.cash_flow = None

        self.power = None

        self.volume = None

        self.reset_data()

    def set_parameter(self, date, weather=None, prices=None):
        self.date = date
        self.weather = weather
        self.prices = prices

    def optimize(self):
        pass

    def reset_data(self):

        self.generation = dict(total=np.zeros((self.T,), float),
                               solar=np.zeros((self.T,), float),
                               wind=np.zeros((self.T,), float),
                               water=np.zeros((self.T,), float),
                               bio=np.zeros((self.T,), float),
                               lignite=np.zeros((self.T,), float),
                               coal=np.zeros((self.T,), float),
                               gas=np.zeros((self.T,), float),
                               nuclear=np.zeros((self.T,), float))

        self.demand = dict(power=np.zeros((self.T,), float),
                           heat=np.zeros((self.T,), float))

        self.cash_flow = dict(profit=np.zeros((self.T,), float),
                              fuel=np.zeros((self.T,), float),
                              emission=np.zeros((self.T,), float),
                              start_ups=np.zeros((self.T,), float))

        self.volume = np.zeros(self.T, float)
        self.power = np.zeros(self.T, float)
