# third party modules
import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS


# model modules
from components.energy_system import EnergySystem as es

from apps.slpP import slpGen as slpP


class PvBatModel(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, lat=50.77, lon=6.09,  pdc0=0.24, azimuth=180, tilt=0):
        super().__init__(t, T, dt)
        # initialize weather for generation calculation
        self.weather = None

        # initialize standard h0 consumer attributes
        self.consumer = dict(date = pd.to_datetime('2018-01-01'),
                             e_el=3000,
                             slpP = slpP(typ=0, refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array','rb')), np.float32)))

        # add photovoltaic system
        pv_system = PVSystem(module_parameters=dict(pdc0=1000 * pdc0, gamma_pdc=-0.004),
                             inverter_parameters=dict(pdc0=1000 * pdc0),
                             surface_tilt=tilt, surface_azimuth=azimuth, albedo=0.25,
                             temperature_model_parameters=TEMPERATURE_MODEL_PARAMETERS['pvsyst']['insulated'],
                             losses_parameters=dict(availability=0, lid=0, shading=1, soiling=1))
        # aggregate in model chain
        self.photovoltaic = ModelChain(pv_system, Location(lat, lon), aoi_model='physical', spectral_model='no_loss',
                                       temperature_model='pvsyst', losses_model='pvwatts', ac_model='pvwatts')

        # add battery storage
        self.battery = dict(v0=0, v_max=3, efficience=0.9)

    def set_parameter(self, date, p_el, v0, weather):
        # set weather parameter for calculation
        self.weather = pd.DataFrame.from_dict(weather)
        self.weather['ghi'] = self.weather['dir'] + self.weather['dif']
        self.weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        self.weather.index = pd.date_range(start=date, periods=len(self.weather), freq='60min')
        # set consumer parameter
        self.date = pd.to_datetime(date)
        self.p_el = p_el
        self.v0 = v0
    #     self.h0.date =
    #
    # def optimize(self):
    #     self.mc.run_model(self.weather)
    #     self.generation.update({'solar': self.mc.ac.to_numpy()/1000})                   # set solar generation [kW]
    #     demand = self.slpP.get_profile(self.date.dayofyear, self.date.dayofweek, self.p_el).reshape((96, 1))
    #     self.demand['power'] = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,))
    #
    #     residual = self.demand['power'] - self.generation['solar']
    #     grid_use, volume = [], []
    #     for r in residual:
    #         if (r >= 0) & (vt == 0):                                                            # keine Nutuzng möglich (leer)
    #             grid_use.append(r)
    #             volume.append(0)
    #             continue
    #         if (r >= 0) & (vt * self.dt * self.bat_eta <= r):                             # nutze Restkapazität
    #             grid_use.append(r - vt * self.dt * data['Bat']['eta'])                              # Rest --> Netz
    #             vt = 0
    #             volume.append(0)
    #             continue
    #         if (r >= 0) & (vt * self.dt * data['Bat']['eta'] >= r):                             # entlade Speicher
    #             grid_use.append(0)
    #             vt -= r * self.dt / data['Bat']['eta']
    #             volume.append(vt)
    #             continue
    #         if (r < 0) & (vt == data['Bat']['vmax']):                                           # keine Nutuzng möglich (voll)
    #             grid_use.append(r)
    #             volume.append(data['Bat']['vmax'])
    #             continue
    #         if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] <= data['Bat']['vmax']):        # lade Speicher
    #             grid_use.append(0)
    #             vt -= r * self.dt * data['Bat']['eta']
    #             volume.append(vt)
    #             continue
    #         if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] >= data['Bat']['vmax']):        # lade bis voll
    #             grid_use.append(r + (data['Bat']['vmax'] - vt) * self.dt / data['Bat']['eta'])      # Rest --> Netz
    #             vt = data['Bat']['vmax']
    #             volume.append(vt)
    #             continue
    #
    # def build(self, data, ts, date):
    #
    #     # Vorbereitung Wetterdaten für die Simulation
    #     weather = pd.DataFrame.from_dict(ts)
    #     weather['ghi'] = weather['dir'] + weather['dif']
    #     weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
    #     weather.index = pd.date_range(start=date, periods=len(weather), freq='60min')
    #
    #
    #
    #     # Berechnung der Erzeugung aus PV auf Basis der Wetterdaten
    #     self.mc.run_model(weather)
    #
    #     self.generation['solar'] = self.mc.ac.to_numpy()/1000                                   # PV-Erzeugung pro Stunde in [kW]
    #     self.demand['power'] = self.getPowerDemand(data, date)                                  # Stromverbrauch pro Stunde in [kW]
    #     self.demand['heat'] = np.asarray(self.getHeatDemand(data, ts)).reshape((-1,))           # Wärmebedarf pro Stunde in [KW]
    #
    #     # maximiere den Eigenverbrauch
    #     residuum =  self.demand['power'] - self.generation['solar']
    #
    #     grid = []                                                                               # Netzbezug
    #     volume = []                                                                             # Speicherfüllstand
    #     vt = data['Bat']['v0']                                                                  # Atkueller Speicherfüllstand in [kWh]
    #
    #     for r in residuum:
    #         if (r >= 0) & (vt == 0):                                                            # keine Nutuzng möglich (leer)
    #             grid.append(r)
    #             volume.append(0)
    #             continue
    #         if (r >= 0) & (vt * self.dt * data['Bat']['eta'] <= r):                             # nutze Restkapazität
    #             grid.append(r - vt * self.dt * data['Bat']['eta'])                              # Rest --> Netz
    #             vt = 0
    #             volume.append(0)
    #             continue
    #         if (r >= 0) & (vt * self.dt * data['Bat']['eta'] >= r):                             # entlade Speicher
    #             grid.append(0)
    #             vt -= r * self.dt / data['Bat']['eta']
    #             volume.append(vt)
    #             continue
    #         if (r < 0) & (vt == data['Bat']['vmax']):                                           # keine Nutuzng möglich (voll)
    #             grid.append(r)
    #             volume.append(data['Bat']['vmax'])
    #             continue
    #         if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] <= data['Bat']['vmax']):        # lade Speicher
    #             grid.append(0)
    #             vt -= r * self.dt * data['Bat']['eta']
    #             volume.append(vt)
    #             continue
    #         if (r < 0) & (vt - r * self.dt * data['Bat']['eta'] >= data['Bat']['vmax']):        # lade bis voll
    #             grid.append(r + (data['Bat']['vmax'] - vt) * self.dt / data['Bat']['eta'])      # Rest --> Netz
    #             vt = data['Bat']['vmax']
    #             volume.append(vt)
    #             continue
    #
    #     data['Bat']['v0'] = vt                                                                  # Atkueller Speicherfüllstand in [kWh]
    #     self.volume = volume                                                                    # Speicherfüllstand [kWh]
    #
    #     for i in self.t:
    #         if grid[i] < -0.7 * data['PV']['maxPower']:                                               # Einspeisung > 70 % der Nennleistung
    #             grid[i] = -0.7 * data['PV']['maxPower']                                               # --> Kappung
    #             self.generation['solar'][i] = self.demand['power'][i] + 0.7 * data['PV']['maxPower']  # Erzeugung PV = 70 % der Nennleistung + Aktueller Bedarf
    #
    #     self.power = np.asarray(grid, np.float).reshape((-1,))                                  # Strombedarf [kW]

if __name__ == "__main__":
    test = PvBatModel()
