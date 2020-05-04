import numpy as np
from components.res_Wind import wind_model
from components.res_PvPark import solar_model
from components.res_runRiver import runRiver_model
from components.res_bioMass import bioMass_model
from aggregation.basic_Port import port_model

class resPort(port_model):

    def __int__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ ='DEM'):
        super().__init__(T, dt, gurobi, date, typ)

    def addToPortfolio(self, name, energysystem):
        data = energysystem[name]

        if data['typ'] == 'wind':                                                       # Wind
            data.update(dict(model=wind_model(t=self.t, T=self.T, dt=self.dt)))
        elif data['typ'] == 'solarsystem' or data['typ'] == 'solarpark':                # Solar
            data.update(dict(model=solar_model(t=self.t, T=self.T, dt=self.dt)))
        elif data['typ'] == 'run-river':
            data.update(dict(model=runRiver_model(t=self.t, T=self.T, dt=self.dt)))
        elif data['typ'] == 'biomass':
            data.update(dict(model=bioMass_model(t=self.t, T=self.T, dt=self.dt)))
        self.energySystems.update(energysystem)

    def buildModel(self, response=[]):
        if len(response) == 0:
            for _, data in self.energySystems.items():
                data['model'].build(data, self.weather, self.date)
        else:
            self.generation['total'] = np.asarray(response).reshape((-1,))

    def optimize(self):
        power = np.zeros_like(self.t)
        try:
            # Wind Onshore-Erzeugung
            pWind = np.asarray([value['model'].generation['wind'] for _, value in self.energySystems.items()], np.float)
            self.generation['wind'] = np.sum(pWind, axis=0)

            # PV-Erzeugung
            pSolar = np.asarray([value['model'].generation['solar'] for _, value in self.energySystems.items()], np.float)
            self.generation['solar'] = np.sum(pSolar, axis=0)

            # Laufwasserkraftwerke
            pWater = np.asarray([value['model'].generation['water'] for _, value in self.energySystems.items()], np.float)
            self.generation['water'] = np.sum(pWater, axis=0)

            # Laufwasserkraftwerke
            pBio = np.asarray([value['model'].generation['bio'] for _, value in self.energySystems.items()], np.float)
            self.generation['bio'] = np.sum(pBio, axis=0)

            # Gesamtleitung im Portfolio
            power = self.generation['wind'] + self.generation['solar'] + self.generation['water'] + self.generation['bio']

        except Exception as e:
            print(e)
        self.generation['total'] = power
        return power

    def fixPlaning(self):
        power = np.zeros_like(self.t)
        try:
            err = np.random.normal(loc=0.013, scale=0.037, size=self.T)
            power = self.generation['total'] / (1 - err)
        except Exception as e:
            print(e)
        self.generation['total'] = power
        return power

if __name__ == "__main__":
    test = resPort()