import numpy as np
from components.res_Wind import wind_model
from components.res_PvPark import solar_model
from aggregation.basic_Port import port_model

class resPort(port_model):

    def __int__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ ='DEM'):
        super().__init__(T, dt, gurobi, date, typ)

    def addToPortfolio(self, name, energysystem):
        data = energysystem[name]

        if data['typ'] == 'wind':               # Wind
            data.update(dict(model=wind_model(t=self.t, T=self.T, dt=self.dt)))
        elif data['typ'] == 'solar':            # Solar
            data.update(dict(model=solar_model(t=self.t, T=self.T, dt=self.dt)))

        self.energySystems.update(energysystem)

    def buildModel(self, response=[]):
        for _, data in self.energySystems.items():
            data['model'].build(data, self.weather, self.date)

    def optimize(self):
        power = np.zeros_like(self.t)
        try:
            pWind = np.asarray([value['model'].powerWind for _, value in self.energySystems.items()],np.float)
            self.pWind = np.sum(pWind, axis=0)
            pSolar = np.asarray([value['model'].powerSolar for _, value in self.energySystems.items()], np.float)
            self.pSolar = np.sum(pSolar, axis=0)
            power = self.pWind + self.pSolar
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
    test = resPort()