import numpy as np
from components.energy_system import EnergySystem as es
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS
import pandas as pd


class solar_model(es):

    def __init__(self,
                 lat=50.77, lon=6.09,                                               # Längen- und Breitengrad
                 pdc0=0.24, azimuth=180, tilt=35,
                 number=-1, temp=False,
                 t=np.arange(24), T=24, dt=1):                                      # Metainfo Zeit t, T, dt
        super().__init__(t, T, dt)

        self.location = Location(lat, lon)
        if temp:
            temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['pvsyst']['freestanding']
        else:
            temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['pvsyst']['insulated']

        self.mySys = PVSystem(module_parameters=dict(pdc0=1000*pdc0, gamma_pdc=-0.004),
                              inverter_parameters=dict(pdc0=1000*pdc0),
                              surface_tilt=tilt, surface_azimuth=azimuth, albedo=0.25,
                              temperature_model_parameters = temperature_model_parameters,
                              losses_parameters=dict(availability=0, lid=0, shading=1, soiling=1))      # https://pvlib-python.readthedocs.io/en/stable/generated/pvlib.pvsystem.pvwatts_losses.html

        self.mc = ModelChain(self.mySys, Location(lat, lon), aoi_model='physical', spectral_model='no_loss',
                             temperature_model='pvsyst', losses_model='pvwatts', ac_model='pvwatts')

        self.number = number

    def build(self, data, ts, date):
        if self.number == -1:
            # Vorbereitung Wetterdaten für die Simulation
            weather = pd.DataFrame.from_dict(ts)
            weather['ghi'] = weather['dir'] + weather['dif']
            weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
            weather.index = pd.date_range(start=date, periods=len(weather), freq='60min')

            # Berechnung der Erzeugung aus PV auf Basis der Wetterdaten
            self.mc.run_model(weather)
            self.generation['solar'] = self.mc.ac.to_numpy()/10**6                      # PV-Erzeugung in [kW]

            # Strombedarf (Einspeisung) [MW]
            self.power = np.asarray(self.generation['solar'], np.float).reshape((-1,))
        elif self.number > 0:
            # Vorbereitung Wetterdaten für die Simulation
            weather = pd.DataFrame.from_dict(ts)
            weather['ghi'] = weather['dir'] + weather['dif']
            weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
            weather.index = pd.date_range(start=date, periods=len(weather), freq='60min')

            # Berechnung der Erzeugung aus PV auf Basis der Wetterdaten
            self.mc.run_model(weather)
            power = self.mc.ac.to_numpy() / 10 ** 6  # PV-Erzeugung in [kW]

            self.generation['solar'] = self.number * np.asarray([min(p, 0.7 * data['maxPower']) for p in power]).reshape((-1,))

            # Strombedarf (Einspeisung) [MW]
            self.power = np.asarray(self.generation['solar'], np.float).reshape((-1,))
        else:
            self.generation['solar'] = np.zeros_like(self.t)
            self.power = np.zeros_like(self.t)
