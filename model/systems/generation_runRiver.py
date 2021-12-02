# third party modules
import os
import numpy as np
import statsmodels.api as sm


# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class RunRiverModel(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, run_river=None):
        super().__init__(t, T, dt)

        # initialize default run river power plant
        if run_river is None:
            run_river = dict(maxPower=50)
        self.run_river = run_river

    def optimize(self):
        random = np.random.uniform(low=0.95, high=0.99, size=self.T)
        power_water = random * self.run_river['Power']
        self.generation['powerWater'] = power_water.reshape((-1,))/10**3
        self.power = power_water.reshape((-1,))/10**3

        return self.power

