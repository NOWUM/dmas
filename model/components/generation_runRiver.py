# third party modules
import os
import numpy as np
import statsmodels.api as sm


# model modules
from components.energy_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class RunRiverModel(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, run_river=None):
        super().__init__(t, T, dt)

        # initialize default run river power plant
        if run_river is None:
            run_river = dict(maxPower=50)
        self.run_river = run_river

        self.ref_values = np.load(open(r'./data/Ref_Run_River.array', 'rb'))
        self.mod = sm.tsa.statespace.SARIMAX(self.ref_values, order=(1, 1, 1), seasonal_order=(1, 0, 1, 24),
                                             simple_differencing=False)

        self.states = np.load(open(r'./data/States_Run_River.array', 'rb'))
        self.params = np.asarray([0.6941, -0.9042, 0.8464, -0.7537, 0.0003])

    def optimize(self):

        i = self.date.dayofyear

        random = self.mod.simulate(params=self.params, nsimulations=24, anchor='start',
                                   initial_state=self.states[int(24*np.random.choice(np.arange(max(i-15,0),
                                                                                               min(i+15, 365)))), :])

        power_water = random*self.run_river['maxPower']
        self.generation['powerWater'] = np.asarray(power_water, np.float).reshape((-1,))/10**3
        self.power = np.asarray(power_water, np.float).reshape((-1,))/10**3

