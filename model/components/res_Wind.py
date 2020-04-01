import numpy as np
from components.basic_EnergySystem import energySystem

class wind_model(energySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1):  # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)

    def build(self, data, ts, date):
        powerWind = []
        z0 = 0.5
        if data['P'] <= 600:
            factor = (np.log(50 / z0) / np.log(2 / z0))
        elif data['P'] <= 3000:
            factor = (np.log(95 / z0) / np.log(2 / z0))
        else:
            factor = (np.log(125 / z0) / np.log(2 / z0))

        for wind in ts['wind']:
            wind = wind * factor
            if wind <= data['take_off']:
                generation = 0
            elif wind >= data['cut_off']:
                generation = 0
            else:
                generation = data['P'] / (1 + np.exp(-data['k'] * (wind - data['x0'])))
            powerWind.append(generation/10**3)
        self.powerWind = np.asarray(powerWind, np.float).reshape((-1,))