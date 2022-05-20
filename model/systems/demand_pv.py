# third party modules
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem

# model modules
from systems.basic_system import EnergySystem
from demandlib.electric_profile import StandardLoadProfile


class HouseholdPvModel(EnergySystem):

    def __init__(self, T: int, demandP: float, maxPower: float, azimuth: int, tilt: int, *args, **kwargs):
        super().__init__(T)

        self.demand_system = StandardLoadProfile(type='household', demandP=demandP)
        self.pv_system = PVSystem(module_parameters=dict(pdc0=maxPower), surface_tilt=tilt, surface_azimuth=azimuth)

    def optimize(self):
        """
        :return: timer series in [kW]
        """
        # -> irradiance unit [W/m²]
        irradiance = self.pv_system.get_irradiance(solar_zenith=self.weather['zenith'],
                                                   solar_azimuth=self.weather['azimuth'],
                                                   dni=self.weather['dni'],
                                                   ghi=self.weather['ghi'],
                                                   dhi=self.weather['dhi'])
        # get generation in [kW]
        solar_power = (irradiance['poa_global'] / 10**3) * self.pv_system.arrays[0].module_parameters['pdc0']
        self.generation['solar'] = solar_power.to_numpy()
        # get demand in [kW]
        self.demand['power'] = self.demand_system.run_model(self.date)

        self.power = np.asarray(self.demand['power'] - self.generation['solar']).flatten()

        return self.power

