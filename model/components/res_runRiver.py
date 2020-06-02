import numpy as np
from components.basic_EnergySystem import energySystem
import statsmodels.api as sm


class runRiver_model(energySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1):  # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)
        self.ref_values = np.load(open(r'./data/Ref_Run_River.array','rb'))
        self.mod = sm.tsa.statespace.SARIMAX(self.ref_values, order=(1, 1, 1), seasonal_order=(1, 0, 1, 24),
                                             simple_differencing=False)

        self.states = np.load(open(r'./data/States_Run_River.array','rb'))
        self.params = np.asarray([0.6941, -0.9042, 0.8464, -0.7537, 0.0003])


    def build(self, data, ts, date):

        i = date.dayofyear

        random = self.mod.simulate(params=self.params, nsimulations=24, anchor='start',
                                   initial_state=self.states[int(24*np.random.choice(np.arange(max(i-15,0),
                                                                                               min(i+15, 365)))), :])

        powerWater = random*data['maxPower']
        self.generation['water'] = np.asarray(powerWater, np.float).reshape((-1,))/10**3
        self.power = np.asarray(powerWater, np.float).reshape((-1,))/10**3

