# third party modules
import os
import numpy as np
import statsmodels.api as sm


# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class RunRiverModel(EnergySystem):

    def __init__(self, T, maxPower, *args, **kwargs):
        super().__init__(T)

        self.run_river = dict(maxPower=maxPower)

    def optimize(self):
        """
        :return: timer series in [kW]
        """
        power_water = np.ones(self.T) * self.run_river['maxPower']
        self.generation['water'] = power_water.flatten()
        self.power = power_water.flatten()

        return self.power

