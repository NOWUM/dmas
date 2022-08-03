# third party modules
import numpy as np
import pandas as pd

from systems.basic_system import EnergySystem


class PortfolioModel:

    def __init__(self, T=24, date='2020-01-01', steps=(0,)):
        '''
        Represents a portfolio of EnergySystems.
        Its capacities, generation and demand is in MW
        '''

        self.date = pd.to_datetime(date)
        self.energy_systems: list[EnergySystem] = []

        self.T, self.t, self.dt = T, np.arange(T), 1

        self.weather = pd.DataFrame()
        self.prices = pd.DataFrame()

        # capacities are in [kW]
        self.capacities = dict(bio=0., coal=0., gas=0., lignite=0., nuclear=0., solar=0.,
                               water=0., wind=0., storage=0.)
        self.generation = None
        self.demand = None
        self.cash_flow = None

        self.power = None
        self.volume = None
        self.committed_power = None

        self.steps = steps

        self.reset_data()

    def set_total_generation(self):
        fuels = [*self.generation.keys()]
        fuels.remove('total')
        self.generation['total'] = np.zeros((self.T,), float)
        for fuel in fuels:
            self.generation['total'] += self.generation[fuel]

    def set_parameter(self, date, weather, prices, committed=None, *agrs, **kwargs):
        self.date = pd.to_datetime(date)
        self.weather = weather
        self.prices = prices
        self.committed_power = committed

    def add_energy_system(self, energy_system):
        """
        adds an energy system to the portfolio
        - power values of the EnergySystem are in kW
        - the capacities of the Portfolio is stored in MW
        """
        pass

    def build_model(self, response=None):
        pass

    def optimize(self):
        power = np.zeros((self.T,))
        return power

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

        self.power = np.zeros(self.T, float)
        self.volume = np.zeros(self.T, float)

