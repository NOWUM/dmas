import numpy as np
from components.basic_EnergySystem import es_model as es

class pvbat_model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t,T,dt,demQ,refSLP,refTemp,factors,parameters)

    def build(self, data, timeseries, date):

        vt = data['Bat']['v0']
        residual = self.getDemHour(data, date) - self.getGenHour(data, timeseries)

        grid = []

        for r in residual:
            if (r >= 0) & (vt == 0):
                grid.append(r)
                continue
            if (r >= 0) & (vt * self.dt * data['Bat']['eta'] <= r):
                grid.append(r - vt * self.dt * data['Bat']['eta'])
                vt = 0
                continue
            if (r >= 0) & (vt * self.dt * data['Bat']['eta'] >= r):
                grid.append(0)
                vt -= r * self.dt / data['Bat']['eta']
                continue
            if (r < 0) & (vt == data['Bat']['vmax']):
                grid.append(r)
                continue
            if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] <= data['Bat']['vmax']):
                grid.append(0)
                vt -= r * self.dt * data['Bat']['eta']
                continue
            if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] >= data['Bat']['vmax']):
                grid.append(r + (data['Bat']['vmax'] - vt) * self.dt / data['Bat']['eta'])
                vt = data['Bat']['vmax']
                continue

        data['Bat']['v0'] = vt
        self.demand = np.asarray(grid, np.float).reshape((-1,))

if __name__ == "__main__":
    test = pvbat_model()
