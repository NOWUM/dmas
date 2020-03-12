import numpy as np
from components.basic_EnergySystem import es_model as es

class h0_model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t,T,dt,demQ,refSLP,refTemp,factors,parameters)

    def getDemHour(self, data, date):
        power = data['demandP'] * 0.1
        base = data['demandP'] * 0.9
        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i+3]) for i in range(0,96,4)], np.float).reshape((-1,)) + base / 8760
        return demand       # -- Profile [kW]

    def build(self, data, timeseries, date):
        self.demand = self.getDemHour(data, date)

class g0_model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t,T,dt,demQ,refSLP,refTemp,factors,parameters)

    def getDemHour(self, data, date):
        power = data['demandP'] * 0.1
        base = data['demandP'] * 0.9
        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,)) + base / 8760
        return demand  # -- Profile [kW]

    def build(self, data, timeseries, date):
        self.demand = self.getDemHour(data, date)

class rlm_model(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):
        super().__init__(t,T,dt,demQ,refSLP,refTemp,factors,parameters)

    def getDemHour(self, data, date):
        power = data['demandP'] * 0.1
        base = data['demandP'] * 0.9
        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, power).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,)) + base / 8760
        return demand  # -- Profile [kW]

    def build(self, data, timeseries, date):
        self.demand = self.getDemHour(data, date)

if __name__ == "__main__":
    testh0 = h0_model()
    testg0 = g0_model()
    testRlm = rlm_model()
