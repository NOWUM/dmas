import numpy as np
from systems.generation_wind import WindModel
from systems.generation_photovoltaic import PvModel
from systems.generation_runRiver import RunRiverModel
from systems.generation_biomass import BioMassModel
from aggregation.portfolio import PortfolioModel


class RenewablePortfolio(PortfolioModel):

    def __init__(self, T=24, date='2020-01-01'):
        super().__init__(T, date)
        self.lock_generation = True

    def add_energy_system(self, energy_system):

        if energy_system['type'] == 'wind':
            energy_system.update({'model': WindModel(self.T, energy_system['turbines'])})
            self.energy_systems.update({energy_system['unitID']: energy_system})
            self.capacities['wind'] += energy_system['maxPower']
        if energy_system['type'] == 'solar':
            energy_system.update({'model': PvModel( self.T, energy_system)})
            self.energy_systems.update({energy_system['unitID']: energy_system})
            self.capacities['solar'] += energy_system['maxPower']
        if energy_system['type'] == 'water':
            energy_system.update({'model': RunRiverModel(self.T, energy_system)})
            self.energy_systems.update({energy_system['unitID']: energy_system})
            self.capacities['water'] += energy_system['maxPower']
        if energy_system['type'] == 'bio':
            energy_system.update({'model': BioMassModel(self.T, energy_system)})
            self.energy_systems.update({energy_system['unitID']: energy_system})
            self.capacities['bio'] += energy_system['maxPower']

    def build_model(self, response=None):

        if response is None:
            self.lock_generation = False
            self.generation['total'] = np.zeros((self.T,))
        else:
            self.lock_generation = True
            self.generation['total'] = np.asarray(response, np.float).reshape((-1,))

        for _, data in self.energy_systems.items():
            data['model'].set_parameter(weather=self.weather, date=self.date)

    def optimize(self):

        for _, data in self.energy_systems.items():
            data['model'].optimize()

        power, solar,  wind,  water, bio = [], [], [], [], []
        for _, value in self.energy_systems.items():
            if value['type'] == 'solar':
                solar.append(value['model'].generation['powerSolar'])
            elif value['typ'] == 'run-river':
                water.append(value['model'].generation['powerWater'])
            elif value['typ'] == 'biomass':
                bio.append(value['model'].generation['powerBio'])
            elif value['typ'] == 'wind':
                wind.append(value['model'].generation['powerWind'])

        self.generation['solar'] = np.sum(np.asarray(solar, np.float), axis=0)
        self.generation['water'] = np.sum(np.asarray(water, np.float), axis=0)
        self.generation['bio'] = np.sum(np.asarray(bio, np.float), axis=0)
        self.generation['wind'] = np.sum(np.asarray(wind, np.float), axis=0)

        if self.lock_generation:
            power_response = self.generation['total']

            for i in self.t:
                power_delta = power_response[i] - power[i]
                if power_delta < 0:
                    self.generation['wind'][i] += power_delta
                    self.generation[wind][i] = np.max((self.generation[wind][i], 0))

        power = self.generation['wind'] + self.generation['solar'] + self.generation['water'] + self.generation['bio']

        self.generation['total'] = np.asarray(power, np.float).reshape((-1,))
        self.power = np.asarray(power, np.float).reshape((-1,))

        return self.power