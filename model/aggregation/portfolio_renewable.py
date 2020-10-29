import numpy as np
import pandas as pd
from components.generation_wind import WindModel
from components.generation_photovoltaic import PvModel
from components.generation_runRiver import RunRiverModel
from components.generation_biomass import BioMassModel
from aggregation.portfolio import PortfolioModel
from scipy import interpolate
from windpowerlib import power_curves


class RenewablePortfolio(PortfolioModel):

    def __init__(self, T=24, dt=1, gurobi=False, date='2020-01-01'):
        super().__init__(T, dt, gurobi, date)

        # initialize tmp. variables to aggregate the wind turbines to a single one (self.windModel)
        self.powerCurve = None                      # smoothed power curve for aggregated turbine
        self.power_curves = []                      # array for all power values of each turbine
        self.wind_speed = np.array([])              # array for all wind speeds of each turbine
        self.hubHeight = 0                          # mean (averaged by power) hub height
        self.totalPower = 0                         # sum over each turbine to calculate the mean hub height

        self.windModel = None                       # aggregated wind turbine

        self.fix = True

    def add_energy_system(self, name, energy_system):

        data = energy_system[name]

        # build wind energy turbine
        if data['typ'] == 'wind':

            model = WindModel(wind_turbine=data)

            self.wind_speed = np.concatenate((model.wind_turbine.power_curve['wind_speed'].to_numpy(), self.wind_speed))

            self.power_curves.append((model.wind_turbine.power_curve['wind_speed'].to_numpy(),
                                      model.wind_turbine.power_curve['value'].to_numpy()))

            self.totalPower += data['maxPower']
            self.hubHeight += data['maxPower']*data['height']

            data.update(dict(model=model))

        # build photovoltaic according to the EEG remuneration and
        elif data['typ'] == 'Pv' :
            data.update(dict(model=PvModel(lat=data['position'][0],
                                           lon=data['position'][1],
                                           photovoltaic=data['PV'])))
        # build open standing photovoltaic
        elif data['typ'] == 'PVPark':
            data.update(dict(model=PvModel(lat=data['position'][0],
                                           lon=data['position'][1],
                                           photovoltaic=data,
                                           open_space=True)))
        # build trade and business photovoltaic
        elif data['typ'] == 'PVTrIn' or data['typ'] == 'Pv70':
            data.update(dict(model=PvModel(lat=data['position'][0],
                                           lon=data['position'][1],
                                           photovoltaic=data,
                                           open_space=False)))
        # build trade and business photovoltaic with 70% Cap
        elif data['typ'] == 'PV70':
            data.update(dict(model=PvModel(lat=data['position'][0],
                                           lon=data['position'][1],
                                           photovoltaic=data,
                                           open_space=False)))
        # build run river power plants
        elif data['typ'] == 'run-river':
            data.update(dict(model=RunRiverModel(run_river=data)))
       # build biomass power plant
        elif data['typ'] == 'biomass':
            data.update(dict(model=BioMassModel(bio_mass=data)))

        self.energy_systems.update(energy_system)

    def aggregate_wind(self):
        self.wind_speed = np.sort(np.unique(self.wind_speed))
        value = np.asarray(np.zeros_like(self.wind_speed), dtype=np.float64)
        for powerCurve in self.power_curves:
            f = interpolate.interp1d(powerCurve[0], powerCurve[1], fill_value=0, bounds_error=False)
            value += np.asarray(f(self.wind_speed), dtype=np.float64)

        self.powerCurve = power_curves.smooth_power_curve(power_curve_wind_speeds=pd.Series(self.wind_speed),
                                                          power_curve_values=pd.Series(value),
                                                          standard_deviation_method='turbulence_intensity',
                                                          turbulence_intensity=0.15,
                                                          mean_gauss=0, wind_speed_range=10)

        self.hubHeight = self.hubHeight / self.totalPower

        self.windModel = WindModel(t=np.arange(24), T=24, dt=1, power_curve=self.powerCurve,
                                   wind_turbine=dict(height=self.hubHeight, diameter=100))

        # delete tmp. variables
        del self.power_curves, self.powerCurve, self.hubHeight, self.totalPower, self.wind_speed

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
            power = solar = wind = water = bio = []
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

