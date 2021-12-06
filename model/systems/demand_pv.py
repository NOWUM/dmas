# third party modules
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS


# model modules
from systems.basic_system import EnergySystem
from demandlib.electric_profile import StandardLoadProfile


class HouseholdPvModel(EnergySystem):

    def __init__(self, T, demandP, maxPower, azimuth, tilt, lat, lon, *args, **kwargs):
        super().__init__(T)

        # initialize weather for generation calculation
        self.weather = None

        self.demand_system = StandardLoadProfile(type='household', demandP=demandP)

        pv_system = PVSystem(module_parameters=dict(pdc0=1000 * maxPower, gamma_pdc=-0.004),
                             inverter_parameters=dict(pdc0=1000 * maxPower),
                             surface_tilt=tilt, surface_azimuth=azimuth, albedo=0.25,
                             temperature_model_parameters=TEMPERATURE_MODEL_PARAMETERS['pvsyst']['insulated'],
                             losses_parameters=dict(availability=0, lid=0, shading=1, soiling=1))
        # aggregate in model chain
        self.pv_system = ModelChain(pv_system, Location(lat, lon), aoi_model='physical', spectral_model='no_loss',
                                    temperature_model='pvsyst', losses_model='pvwatts', ac_model='pvwatts')

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        # set weather parameter for calculation
        self.weather = weather
        # set prices
        self.prices = prices

    def optimize(self):
        self.pv_system.run_model(self.weather)
        # get generation in [kW]
        self.generation['solar'] = self.pv_system.results.ac.to_numpy()/1000
        # get demand in [kW]
        self.demand['power'] = self.demand_system.run_model(self.date)
        # get grid usage in [kW]
        grid_use = self.demand['power'] - self.generation['solar']
        # gird usage in [kW]
        self.power = np.asarray(grid_use, np.float).reshape((-1,))

        return self.power