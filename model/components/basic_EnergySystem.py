import numpy as np
from apps.slpP import slpGen as slpP
from apps.slpQ import slpGen as slpQ

class es_model:

    def __init__(self, t=np.arange(24), T=24, dt=1, demQ=1000,
                 refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32),
                 refTemp=np.asarray(np.load(open(r'./data/Ref_Temp.array', 'rb')), np.float32),
                 factors=np.asarray(np.load(open(r'./data/Ref_Factors.array', 'rb')), np.float32),
                 parameters=np.asarray([2.8, -37, 5.4, 0.17], np.float32)):

        self.t = t
        self.T = T
        self.dt = dt

        self.slpP = slpP(typ=0, refSLP=refSLP)
        self.slpQ = slpQ(demandQ=demQ, parameters=parameters.reshape((-1,)),
                         refTemp=np.asarray(refTemp,np.float32).reshape((-1,)),
                         factors=np.asarray(factors,np.float32).reshape((24, -1)))

        self.demand = np.zeros_like(self.t)
        self.genWind = np.zeros_like(self.t)
        self.genSolar = np.zeros_like(self.t)

    def getDemHour(self, data, date):
        demand = self.slpP.get_profile(date.dayofyear, date.dayofweek, data['demandP']).reshape((96, 1))
        demand = np.asarray([np.mean(demand[i:i+3]) for i in range(0,96,4)], np.float).reshape((-1,))
        return demand       # -- Profile [kW]

    def getGenHour(self, data, timeseries):
        rad = np.asarray(timeseries['dif']) + np.asarray(timeseries['dir'])
        return 7 * data['PV']['peakpower'] * rad * data['PV']['eta'] / 1000  # -- Profile [kW]

    def getHeatHour(self,timeseries):
        return self.slpQ.get_profile(np.asarray(timeseries['temp'], np.float32))

    def build(self, name, data, timeseries):
        pass

