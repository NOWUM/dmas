import numpy as np
from components.basic_EnergySystem import energySystem

class wind_model(energySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1):  # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)

    def build(self, data, ts, date):
        powerWind = []
        z0 = 0.5
        factor = (data['height']/2)**0.14
        #factor = (np.log(data['height'] / z0) / np.log(2 / z0))

        for wind in ts['wind']:
            wind = wind * factor
            if wind <= data['take_off']:
                generation = 0
            elif wind >= data['cut_off']:
                generation = 0
            else:
                generation = data['maxPower'] / (1 + np.exp(-data['k'] * (wind - data['x0'])))
            powerWind.append(generation/10**3)
        self.generation['wind'] = np.asarray(powerWind, np.float).reshape((-1,))
        self.power = np.asarray(powerWind, np.float).reshape((-1,))