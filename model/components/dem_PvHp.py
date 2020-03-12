import numpy as np
from components.basic_EnergySystem import es_model as es

class pvwp_model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t,T,dt,demQ,refSLP,refTemp,factors,parameters)

    def build(self, data, timeseries, date):

        residual = self.getDemHour(data, date) - self.getGenHour(data, timeseries)
        heat = self.getHeatHour(timeseries)
        vt = data['tank']['v0']

        grid = []

        i = 0
        for r in residual:
            if (r >= 0) & (vt == 0) | (heat[i] > vt):
                q_wp = heat[i]
                grid.append(r + q_wp/data['WP']['cop'])
                i += 1
                continue
            if (r >= 0) & (heat[i] <= vt):
                vt -= heat[i]
                grid.append(r)
                i += 1
                continue
            if (r <= 0):
                d_power = heat[i]/data['WP']['cop']
                if (d_power < r) & (vt - (r + d_power)*data['WP']['cop'] < data['tank']['vmax']):
                    vt -= (r + d_power)*data['WP']['cop']
                    grid.append(0)
                    i += 1
                    continue
                if (d_power < r) & (vt - (r + d_power)*data['WP']['cop'] >= data['tank']['vmax']):
                    grid.append((r + d_power)-(data['tank']['vmax']-vt)/data['WP']['cop'])
                    vt = data['tank']['vmax']
                    i += 1
                    continue
                if (d_power > r):
                    grid.append(d_power+r)
                    i += 1
                    continue

        data['tank']['v0'] = vt
        self.demand = np.asarray(grid, np.float).reshape((-1,))

if __name__ == "__main__":
    test = pvwp_model()
