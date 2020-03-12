import numpy as np
from components.basic_EnergySystem import es_model

class solar_model(es_model):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t, T, dt, demQ, refSLP, refTemp, factors, parameters)

    def build(self, data, timeseries, date):
        day = []

        rad = []
        for i in range(len(timeseries['dif'])):
            rad.append(timeseries['dif'][i] + timeseries['dir'][i])
        for solar in rad:
            area = data['area'] * data['peakpower']

            generation = solar * data['eta'] * area

            day.append(generation/10**6)

        self.genSolar = np.asarray(day, np.float).reshape((-1,))
