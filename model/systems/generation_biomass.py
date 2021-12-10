# third party modules
import os
import numpy as np

# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class BioMassModel(EnergySystem):

    def __init__(self, T, maxPower, *args, **kwargs):
        super().__init__(T)

        self.bio_mass = dict(maxPower=maxPower)

    def optimize(self):
        random = np.random.uniform(low=0.95, high=0.99, size=self.T)
        power_bio = random * self.bio_mass['maxPower']
        self.generation['bio'] = power_bio.reshape((-1,))/10**3
        self.power = power_bio.reshape((-1,))/10**3
        self.generation['total'] = self.generation['bio']

        return self.power




