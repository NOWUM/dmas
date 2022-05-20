# third party modules
import os
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem

# model modules
from systems.basic_system import EnergySystem as es
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class PvModel(es):

    def __init__(self, T, maxPower, azimuth, tilt, *args, **kwargs):
        super().__init__(T)

        self.pv_system = PVSystem(module_parameters=dict(pdc0=maxPower), surface_tilt=tilt, surface_azimuth=azimuth)

    def optimize(self):
        irradiance = self.pv_system.get_irradiance(solar_zenith=self.weather['zenith'],
                                                   solar_azimuth=self.weather['azimuth'],
                                                   dni=self.weather['dni'],
                                                   ghi=self.weather['ghi'],
                                                   dhi=self.weather['dhi'])
        # get generation in [kW]
        solar_power = (irradiance['poa_global'] / 10**3) * self.pv_system.arrays[0].module_parameters['pdc0']
        self.generation['solar'] = solar_power.to_numpy()
        self.power = self.generation['solar']

        return self.power
