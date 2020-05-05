import numpy as np
from components.basic_EnergySystem import energySystem

class runRiver_model(energySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1):  # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)

    def build(self, data, ts, date):

        random = np.random.uniform(low=0.95, high=1, size=self.T)
        powerWater = random*data['maxPower']
        self.generation['water'] = np.asarray(powerWater, np.float).reshape((-1,))/10**3
        self.power = np.asarray(powerWater, np.float).reshape((-1,))/10**3