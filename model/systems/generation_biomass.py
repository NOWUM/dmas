# third party modules
import os
import numpy as np

# model modules
from systems.basic_system import EnergySystem



class BioMassModel(EnergySystem):

    def __init__(self, T: int, maxPower: float, *args, **kwargs):
        super().__init__(T)

        self.bio_mass = dict(maxPower=maxPower)

    def optimize(self):
        """
        :return: timer series in [kW]
        """
        power_bio = np.ones(self.T) * self.bio_mass['maxPower']
        self.generation['bio'] = power_bio.flatten()
        self.power = power_bio.flatten()

        return self.power




