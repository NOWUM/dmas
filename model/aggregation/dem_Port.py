import numpy as np
from components.dem_PvBat import pvbat_model as pvBattery
from components.dem_PvHp import pvwp_model as pvHeatpump
from components.dem_PvSolo import pv_model as pvSolo
from components.dem_Consumer import h0_model, g0_model, rlm_model
from aggregation.basic_Port import port_model

class demPort(port_model):

    def __int__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ ='DEM'):
        super().__init__(T, dt, gurobi, date, typ)

    def addToPortfolio(self, name, energysystem):
        data = energysystem[name]

        # -- Prosumer
        if data['typ'] == 'PvBat':
            data.update(dict(model=pvBattery(t=self.t, T=self.T, dt=self.dt, parameters=data['para'], demQ=data['demandQ'],
                                             refTemp=self.Ref_Temperature, factors=self.Ref_factors, refSLP=self.Ref_H0)))
        elif data['typ'] == 'PvWp':
            data.update(dict(model=pvHeatpump(t=self.t, T=self.T, dt=self.dt, parameters=data['para'], demQ=data['demandQ'],
                                              refTemp=self.Ref_Temperature, factors=self.Ref_factors, refSLP=self.Ref_H0)))
        elif data['typ'] == 'Pv':
            data.update(dict(model=pvSolo(t=self.t, T=self.T, dt=self.dt, parameters=data['para'], demQ=data['demandQ'],
                                          refTemp=self.Ref_Temperature, factors=self.Ref_factors, refSLP=self.Ref_H0)))
        # -- Consumer
        elif data['typ'] == 'H0':
            data.update(dict(model=h0_model(t=self.t, T=self.T, dt=self.dt, refSLP=self.Ref_H0)))
        elif data['typ'] == 'G0':
            data.update(dict(model=g0_model(t=self.t, T=self.T, dt=self.dt, refSLP=self.Ref_G0)))
        elif data['typ'] == 'RLM':
            data.update(dict(model=rlm_model(t=self.t, T=self.T, dt=self.dt, refSLP=self.Ref_Rlm)))

        self.energySystems.update(energysystem)

    def buildModel(self, response=[]):
        for _, data in self.energySystems.items():
            data['model'].build(data, self.weather, self.date)

    def optimize(self):
        power = np.zeros_like(self.t)
        try:
            power = np.asarray([value['model'].powerDemand for _, value in self.energySystems.items()], np.float)
            power = np.sum(power, axis=0)
        except Exception as e:
            print(e)
        self.power = power
        return power

    def fixPlaning(self):
        power = np.zeros_like(self.t)
        try:
            err = np.random.normal(loc=0.013, scale=0.037, size=self.T)
            power = self.power / (1 - err)
        except Exception as e:
            print(e)
        self.power = power
        return power

if __name__ == "__main__":
    test = demPort()