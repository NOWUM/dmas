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

    def buildModel(self, response=[]):

        self.windSpeed = np.sort(np.unique(self.windSpeed))
        value = np.asarray(np.zeros_like(self.windSpeed), dtype=np.float64)
        for powerCurve in self.powerCurves:
            f = interpolate.interp1d(powerCurve[0], powerCurve[1], fill_value=0, bounds_error=False)
            value += np.asarray(f(self.windSpeed), dtype=np.float64)

        powerCurve = power_curves.smooth_power_curve(power_curve_wind_speeds=pd.Series(self.windSpeed),
                                                     power_curve_values=pd.Series(value),
                                                     standard_deviation_method='turbulence_intensity',
                                                     turbulence_intensity=0.15,
                                                     mean_gauss=0, wind_speed_range=10)


        self.hubHeight = self.hubHeight/self.totalPower

        self.windModel = wind_model('Area', hub_height=self.hubHeight, rotor_diameter=100,
                                    t=np.arange(24), T=24, dt=1, power_curve=powerCurve)

        self.windModel.build({}, self.weather, self.date)

        if len(response) == 0:
            self.generation['total'] = np.zeros_like(self.t)
            for _, data in self.energySystems.items():
                data['model'].build(data, self.weather, self.date)
        else:
            self.generation['total'] = np.asarray(response).reshape((-1,))

    def optimize(self):
        power = self.generation['total']                    # Leistungsbilanz des Gebietes
        try:
            # Wind Erzeugung
            # pWind = np.asarray([value['model'].generation['wind'] for _, value in self.energySystems.items()], np.float)
            self.generation['wind'] = self.windModel.generation['wind']

            # PV-Erzeugung
            pSolar = np.asarray([value['model'].generation['solar'] if value['typ'] != 'Pv' else value['model'].generation['solar'] * value['EEG']
                                 for _, value in self.energySystems.items()], np.float)
            self.generation['solar'] = np.sum(pSolar, axis=0)

            # Laufwasserkraftwerke
            pWater = np.asarray([value['model'].generation['water'] for _, value in self.energySystems.items()], np.float)
            self.generation['water'] = np.sum(pWater, axis=0)

            # Laufwasserkraftwerke
            pBio = np.asarray([value['model'].generation['bio'] for _, value in self.energySystems.items()], np.float)
            self.generation['bio'] = np.sum(pBio, axis=0)

            if np.sum(self.generation['total']) == 0:
                # Gesamtleitung im Portfolio
                power = self.generation['wind'] + self.generation['solar'] + self.generation['water'] + self.generation['bio']
            else:
                power = self.generation['total']
                self.generation['wind'] = power - self.generation['solar'] - self.generation['water'] - self.generation['bio']

        except Exception as e:
            print(e)
        self.generation['total'] = power
        self.power = power
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
