import numpy as np
import pandas as pd
from components.generation_wind import WindModel
from components.generation_photovoltaic import PvModel
from components.generation_runRiver import RunRiverModel
from components.generation_biomass import BioMassModel
from aggregation.portfolio import PortfolioModel
from scipy import interpolate
from windpowerlib import power_curves
import copy


class RenewablePortfolio(PortfolioModel):

    def __init__(self, T=24, dt=1, gurobi=False, date='2020-01-01'):
        super().__init__(T, dt, date)

        self.fix = True

    def add_energy_system(self, energy_system):

        if energy_system['type'] == 'wind':
            energy_system.update({'model': WindModel(self.t, self.T, self.dt, energy_system['turbines'])})
            self.energy_systems.update({energy_system['unitID']: energy_system})
        if energy_system['type'] == 'solar':
            energy_system.update({'model': PvModel(self.t, self.T, self.dt, energy_system)})
            self.energy_systems.update({energy_system['unitID']: energy_system})
        if energy_system['type'] == 'water':
            energy_system.update({'model': RunRiverModel(self.t, self.T, self.dt, energy_system)})
            self.energy_systems.update({energy_system['unitID']: energy_system})
        if energy_system['type'] == 'bio':
            energy_system.update({'model': BioMassModel(self.t, self.T, self.dt, energy_system)})
            self.energy_systems.update({energy_system['unitID']: energy_system})

    def build_model(self, response=None):

        if response is None:
            self.fix = False
            self.generation['powerTotal'] = np.zeros_like(self.t)
        else:
            self.fix = True
            self.generation['powerTotal'] = np.asarray(response, np.float).reshape((-1,))

        for _, data in self.energy_systems.items():
            if data['typ'] != 'wind':
                data['model'].set_parameter(weather=self.weather, date=self.date)
            elif data['typ'] == 'wind' and self.windModel is None:
                data['model'].set_parameter(weather=self.weather, date=self.date)

        if self.windModel is not None:
            self.windModel.set_parameter(weather=self.weather, date=self.date)

    def optimize(self):

        # optimize each energy system
        for _, data in self.energy_systems.items():
            if data['typ'] != 'wind':
                data['model'].optimize()
            elif data['typ'] == 'wind' and self.windModel is None:
                data['model'].optimize()
        if self.windModel is not None:
            self.windModel.optimize()

        try:
            power, solar,  wind,  water, bio = [], [], [], [], []
            for _, value in self.energy_systems.items():
                # cases for photovoltaic
                if value['typ'] == 'Pv':
                    solar.append(value['model'].generation['powerSolar'] * value['EEG'])
                elif value['typ'] == 'PVPark' or value['typ'] == 'PVTrIn':
                    solar.append(value['model'].generation['powerSolar'])
                elif value['typ'] == 'PV70':
                    tmp = np.asarray([min(p, 0.7 * value['maxPower'])
                                      for p in value['model'].generation['powerSolar']]).reshape((-1,))
                    solar.append(tmp * value['number'])
                # case run-river
                elif value['typ'] == 'run-river':
                    water.append(value['model'].generation['powerWater'])
                # case biomass
                elif value['typ'] == 'biomass':
                    bio.append(value['model'].generation['powerBio'])
                # case wind
                elif value['typ'] == 'wind' and self.windModel is None:
                    wind.append(value['model'].generation['powerWind'])

            if self.windModel is not None:
                wind.append(self.windModel.generation['powerWind'])

            self.generation['powerSolar'] = np.sum(np.asarray(solar, np.float), axis=0)
            self.generation['powerWater'] = np.sum(np.asarray(water, np.float), axis=0)
            self.generation['powerBio'] = np.sum(np.asarray(bio, np.float), axis=0)
            self.generation['powerWind'] = np.sum(np.asarray(wind, np.float), axis=0)
            power = self.generation['powerWind'] + self.generation['powerSolar'] + self.generation['powerWater'] + \
                    self.generation['powerBio']

            if self.fix:                                                       # result dayAhead
                power_response = self.generation['powerTotal']

                for i in self.t:
                    power_delta = power_response[i] - power[i]
                    if power_delta < 0:
                        if self.generation['powerWind'][i] >= np.abs(power_delta):
                            self.generation['powerWind'][i] += power_delta
                        else:
                            power_delta += self.generation['powerWind'][i]
                            self.generation['powerWind'][i] = 0.0

                power = self.generation['powerWind'] + self.generation['powerSolar'] + self.generation['powerWater'] + \
                        self.generation['powerBio']

        except Exception as e:
            print(e)

        self.generation['powerTotal'] = np.asarray(power, np.float).reshape((-1,))
        self.power = np.asarray(power, np.float).reshape((-1,))

        return self.power

if __name__ == "__main__":
    pass

