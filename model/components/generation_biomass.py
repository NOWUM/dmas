# third party modules
import os
import numpy as np


# model modules
from components.energy_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class BioMassModel(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, bio_mass=None):
        super().__init__(t, T, dt)

        # initialize default biomass power plant
        if bio_mass is None:
            bio_mass = dict(maxPower=50)
        self.bio_mass = bio_mass

    def optimize(self):
        random = np.random.uniform(low=0.95, high=1, size=self.T)
        power_bio = random * self.bio_mass['maxPower']*0.65
        self.generation['powerBio'] = power_bio.reshape((-1,))/10**3
        self.power = power_bio.reshape((-1,))/10**3




