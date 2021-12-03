# third party modules
import os
import numpy as np


# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class BioMassModel(EnergySystem):

    def __init__(self, T, bio_mass=None):
        super().__init__(T)

        # initialize default biomass power plant
        if bio_mass is None:
            bio_mass = dict(Power=50)
        self.bio_mass = bio_mass

    def optimize(self):
        random = np.random.uniform(low=0.95, high=0.99, size=self.T)
        power_bio = random * self.bio_mass['Power']
        self.generation['powerBio'] = power_bio.reshape((-1,))/10**3
        self.power = power_bio.reshape((-1,))/10**3

        return self.power




