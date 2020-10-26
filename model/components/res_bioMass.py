import numpy as np
from components.energy_system import EnergySystem

class bioMass_model(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1):  # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)

    def build(self, data, ts, date):

        random = np.random.uniform(low=0.95, high=1, size=self.T)
        powerBio = random*data['maxPower']*0.65
        self.generation['bio'] = powerBio.reshape((-1,))/10**3
        self.power = powerBio.reshape((-1,))/10**3
