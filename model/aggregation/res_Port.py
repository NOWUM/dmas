import numpy as np
import pandas as pd
from components.res_Wind import wind_model
from components.res_PvPark import solar_model
from components.res_runRiver import runRiver_model
from components.res_bioMass import bioMass_model
from aggregation.basic_Port import port_model
from scipy import interpolate
from windpowerlib import power_curves


class resPort(port_model):

    def __init__(self, T=24, dt=1, gurobi=False, date='2020-01-01', typ='RES'):
        super().__init__(T, dt, gurobi, date, typ)
        self.powerCurve = None
        self.windSpeed = np.array([])
        self.hubHeight = 0
        self.totalPower = 0
        self.powerCurves = []
        self.windModel = None

    def addToPortfolio(self, name, energysystem):

        data = energysystem[name]

        # Windkraftanlagen
        if data['typ'] == 'wind':

            model = wind_model(turbine_type=data['turbine_type'],
                               hub_height=data['height'],
                               rotor_diameter=data['diameter'],
                               t=self.t,                            # Array mit Zeitschritten
                               T=self.T,                            # Anzahl an Zeitschritten
                               dt=self.dt)

            self.windSpeed = np.concatenate((model.windTurbine.power_curve['wind_speed'].to_numpy(), self.windSpeed))

            self.powerCurves.append((model.windTurbine.power_curve['wind_speed'].to_numpy(),
                                   model.windTurbine.power_curve['value'].to_numpy()))

            self.totalPower += data['maxPower']
            self.hubHeight += data['maxPower']*data['height']

            data.update(dict(model=model))

        # Photovoltaik-Dachanlagen EEG
        elif data['typ'] == 'Pv':
            data.update(dict(model=solar_model(lat=data['position'][0],                 # Längengrad
                                               lon=data['position'][1],                 # Breitengrad
                                               pdc0=data['PV']['maxPower'],             # Nennleistung
                                               azimuth=data['PV']['azimuth'],           # Ausrichtung (180° = Süd)
                                               tilt=data['PV']['tilt'],                 # Dachneigung
                                               temp=False,                              # Dachanlage
                                               t=self.t,                                # Array mit Zeitschritten
                                               T=self.T,                                # Anzahl an Zeitschritten
                                               dt=self.dt)))                            # Zeitschrittlänge

        # Freiflächen-PV
        elif data['typ'] == 'PVPark':
            data.update(dict(model=solar_model(lat=data['position'][0],                 # Längengrad
                                               lon=data['position'][1],                 # Breitengrad
                                               pdc0=data['maxPower'],                   # Nennleistung
                                               azimuth=data['azimuth'],                 # Ausrichtung (180° = Süd)
                                               tilt=data['tilt'],                       # Dachneigung
                                               temp=True,                               # Freistehende Anlage
                                               t=self.t,                                # Array mit Zeitschritten
                                               T=self.T,                                # Anzahl an Zeitschritten
                                               dt=self.dt)))                            # Zeitschrittlänge

        # Gewerblich genutzte PV
        elif data['typ'] == 'PVTrIn':
            data.update(dict(model=solar_model(lat=data['position'][0],                 # Längengrad
                                               lon=data['position'][1],                 # Breitengrad
                                               pdc0=data['maxPower'],                   # Nennleistung
                                               azimuth=data['azimuth'],                 # Ausrichtung (180° = Süd)
                                               tilt=data['tilt'],                       # Dachneigung
                                               temp=False,                              # Dachanlage
                                               t=self.t,                                # Array mit Zeitschritten
                                               T=self.T,                                # Anzahl an Zeitschritten
                                               dt=self.dt)))                            # Zeitschrittlänge

        # Gewerblich genutzte PV mit 70 % Begrenzung
        elif data['typ'] == 'PV70':
            data.update(dict(model=solar_model(lat=data['position'][0],                 # Längengrad
                                               lon=data['position'][1],                 # Breitengrad
                                               pdc0=data['maxPower'],                   # Nennleistung
                                               azimuth=data['azimuth'],                 # Ausrichtung (180° = Süd)
                                               tilt=data['tilt'],                       # Dachneigung
                                               number=data['number'],                   # Anzahl der Anlagen
                                               t=self.t,                                # Array mit Zeitschritten
                                               T=self.T,                                # Anzahl an Zeitschritten
                                               dt=self.dt)))                            # Zeitschrittlänge
        # Laufwasskraftwerke
        elif data['typ'] == 'run-river':
            data.update(dict(model=runRiver_model(t=self.t,                             # Array mit Zeitschritten
                                                  T=self.T,                             # Anzahl an Zeitschritte
                                                  dt=self.dt)))                         # Zeitschrittlänge

       # Biomassekraftwerke
        elif data['typ'] == 'biomass':
            data.update(dict(model=bioMass_model(t=self.t,                              # Array mit Zeitschritten
                                                 T=self.T,                              # Anzahl an Zeitschritte
                                                 dt=self.dt)))                          # Zeitschrittlänge

        self.energySystems.update(energysystem)


    def mergeWind(self):
        if len(self.powerCurves) > 0:
            self.windSpeed = np.sort(np.unique(self.windSpeed))
            value = np.asarray(np.zeros_like(self.windSpeed), dtype=np.float64)
            for powerCurve in self.powerCurves:
                f = interpolate.interp1d(powerCurve[0], powerCurve[1], fill_value=0, bounds_error=False)
                value += np.asarray(f(self.windSpeed), dtype=np.float64)

            self.powerCurve = power_curves.smooth_power_curve(power_curve_wind_speeds=pd.Series(self.windSpeed),
                                                              power_curve_values=pd.Series(value),
                                                              standard_deviation_method='turbulence_intensity',
                                                              turbulence_intensity=0.15,
                                                              mean_gauss=0, wind_speed_range=10)

            self.hubHeight = self.hubHeight / self.totalPower

            self.windModel = wind_model('Area', hub_height=self.hubHeight, rotor_diameter=100,
                                        t=np.arange(24), T=24, dt=1, power_curve=self.powerCurve)

    def buildModel(self, response=[]):

        self.generation['powerTotal'] = np.zeros_like(self.t)

        if len(response) == 0:
            if self.powerCurve is not None:
                self.windModel.build({}, self.weather, self.date)
            for _, data in self.energySystems.items():
                if data['typ'] != 'wind':
                    data['model'].build(data, self.weather, self.date)
        else:
            self.generation['powerTotal'] = np.asarray(response).reshape((-1,))


    def optimize(self):
        try:
            if np.sum(self.generation['powerTotal']) == 0:          # opt. dayAhead
                power = np.zeros_like(self.t)
                # generation wind
                if self.powerCurve is not None:
                    self.generation['powerWind'] = self.windModel.generation['wind']

                # generation pv
                p_solar = np.asarray([value['model'].generation['solar'] if value['typ'] != 'Pv' else
                                     value['model'].generation['solar'] * value['EEG']
                                     for _, value in self.energySystems.items()], np.float)
                self.generation['powerSolar'] = np.sum(p_solar, axis=0)

                # generation run river
                p_water = np.asarray([value['model'].generation['water'] for _, value in self.energySystems.items()], np.float)
                self.generation['powerWater'] = np.sum(p_water, axis=0)

                # generation biomass
                p_bio = np.asarray([value['model'].generation['bio'] for _, value in self.energySystems.items()], np.float)
                self.generation['powerBio'] = np.sum(p_bio, axis=0)

                # sum generation
                power = self.generation['powerWind'] + self.generation['powerSolar'] \
                        + self.generation['powerWater'] + self.generation['powerBio']

            else:                                                  # result dayAhead
                power_da = self.generation['powerTotal']
                power = self.generation['powerWind'] + self.generation['powerSolar'] \
                        + self.generation['powerWater'] + self.generation['powerBio']

                for i in self.t:
                    power_delta = power[i] - power_da[i]
                    if power_delta > 0:
                        if self.generation['powerWind'][i] >= power_delta:
                            self.generation['powerWind'] -= power_delta
                        else:
                            power_delta -= self.generation['powerWind'][i]
                            self.generation['powerWind'][i] = 0.0

                power = self.generation['powerWind'] + self.generation['powerSolar'] \
                        + self.generation['powerWater'] + self.generation['powerBio']

        except Exception as e:
            print(e)

        self.generation['powerTotal'] = np.asarray(power, np.float).reshape((-1,))
        self.power = np.asarray(power, np.float).reshape((-1,))

        return self.power


if __name__ == "__main__":
    pass

