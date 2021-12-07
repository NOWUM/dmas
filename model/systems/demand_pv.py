# third party modules
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem

# model modules
from systems.basic_system import EnergySystem
from demandlib.electric_profile import StandardLoadProfile


class HouseholdPvModel(EnergySystem):

    def __init__(self, T, demandP, maxPower, azimuth, tilt, lat, lon, *args, **kwargs):
        super().__init__(T)

        # initialize weather for generation calculation
        self.weather = None

        self.demand_system = StandardLoadProfile(type='household', demandP=demandP)

        self.pv_system = PVSystem(module_parameters=dict(pdc0=maxPower),
                                  surface_tilt=tilt, surface_azimuth=azimuth)

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        # set weather parameter for calculation
        self.weather = weather
        # set prices
        self.prices = prices

    def optimize(self):
        irradiance = self.pv_system.get_irradiance(solar_zenith=self.weather['zenith'],
                                                   solar_azimuth=self.weather['azimuth'],
                                                   dni=self.weather['dni'],
                                                   ghi=self.weather['ghi'],
                                                   dhi=self.weather['dhi'])
        # get generation in [kW]
        solar_power = irradiance['poa_global'] * 0.14 * self.pv_system.arrays[0].module_parameters['pdc0'] * 7
        self.generation['solar'] = solar_power.to_numpy()/1000
        # get demand in [kW]
        self.demand['power'] = self.demand_system.run_model(self.date)
        # get grid usage in [kW]
        grid_use = self.demand['power'] - self.generation['solar']
        # gird usage in [kW]
        self.power = np.asarray(grid_use, np.float).reshape((-1,))

        return self.power