import numpy as np
from components.basic_EnergySystem import es_model

class wind_model(es_model):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t, T, dt, demQ, refSLP, refTemp, factors, parameters)
        z0 = 0.5
        self.factor = (np.log(110 / z0) / np.log(2 / z0))

    def build(self, data, timeseries, date):
        day = []

        for wind in timeseries['wind']:
            wind = wind * self.factor
            if wind <= data['take_off']:
                generation = 0
            elif wind >= data['cut_off']:
                generation = 0
            else:
                generation = data['P'] / (1 + np.exp(-data['k'] * (wind - data['x0'])))

            day.append(generation/10**3)

        self.genWind = np.asarray(day, np.float).reshape((-1,))
