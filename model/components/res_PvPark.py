import numpy as np
from components.basic_EnergySystem import energySystem as es

class solar_model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1):  # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)

    def build(self, data, ts, date):
        powerSolar = []
        rad = [(ts['dif'][i] + ts['dir'][i]) for i in range(len(ts['dif']))]
        for solar in rad:
            area = data['area'] * data['maxPower']
            generation = solar * data['eta'] * area
            powerSolar.append(generation/10**6)

        self.generation['solar'] = np.asarray(powerSolar, np.float).reshape((-1,))
        self.power = np.asarray(powerSolar, np.float).reshape((-1,))