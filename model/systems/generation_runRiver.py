# third party modules
import numpy as np

# model modules
from systems.basic_system import EnergySystem


class RunRiverModel(EnergySystem):

    def __init__(self, T, maxPower, *args, **kwargs):
        super().__init__(T)

        self.run_river = dict(maxPower=maxPower)

    def optimize(self, date=None, weather=None, prices=None, steps=None):
        """
        :return: timer series in [kW]
        """
        power_water = np.ones(self.T) * self.run_river['maxPower']
        self.generation['water'] = power_water.flatten()
        self.power = power_water.flatten()

        return self.power
