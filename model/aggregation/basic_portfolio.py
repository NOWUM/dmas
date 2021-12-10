# third party modules
import numpy as np
import pandas as pd


class PortfolioModel:

    def __init__(self, T=24, date='2020-01-01'):

        self.date = pd.to_datetime(date)
        self.energy_systems = []

        self.T, self.t, self.dt = T, np.arange(T), 1

        self.weather = {}
        self.prices = {}

        self.capacities = None
        self.generation = None
        self.demand = None
        self.cash_flow = None

        self.power = None

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

        self.capacities = dict(bio=0., coal=0., gas=0., lignite=0., nuclear=0., solar=0.,
                               water=0., wind=0., storage=0.)

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

        self.cash_flow = dict(fuel=np.zeros((self.T,), float),
                              emission=np.zeros((self.T,), float),
                              start_ups=np.zeros((self.T,), float))

        self.power = np.zeros(self.T, float)

