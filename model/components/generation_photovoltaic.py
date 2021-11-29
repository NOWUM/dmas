# third party modules
import os
import numpy as np
import pandas as pd
from pvlib.pvsystem import PVSystem
from pvlib.location import Location
from pvlib.modelchain import ModelChain
from pvlib.temperature import TEMPERATURE_MODEL_PARAMETERS

# model modules
from components.energy_system import EnergySystem as es
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class PvModel(es):

    def __init__(self, t, T, dt, photovoltaic=None):
        super().__init__(t, T, dt)

        # initialize default photovoltaic
        if photovoltaic is None:
            photovoltaic = dict(maxPower=2.9, azimuth=180, tilt=30, lat=50, lon=5, open_space=True, limited=100)

        self.location = Location(photovoltaic['lat'], photovoltaic['lat'])
        if photovoltaic['open_space']:
            temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['pvsyst']['freestanding']
        else:
            temperature_model_parameters = TEMPERATURE_MODEL_PARAMETERS['pvsyst']['insulated']
        self.maxPower = photovoltaic['maxPower'] * 10**3 * photovoltaic['limited']
        # add photovoltaic system
        pv_system = PVSystem(module_parameters=dict(pdc0=1000 * photovoltaic['maxPower'], gamma_pdc=-0.004),
                             inverter_parameters=dict(pdc0=1000 * photovoltaic['maxPower']),
                             surface_tilt=photovoltaic['tilt'], surface_azimuth=photovoltaic['azimuth'], albedo=0.25,
                             temperature_model_parameters=temperature_model_parameters,
                             losses_parameters=dict(availability=0, lid=0, shading=1, soiling=1))

        self.photovoltaic = ModelChain(pv_system, self.location, aoi_model='physical', spectral_model='no_loss',
                                       temperature_model='pvsyst', losses_model='pvwatts', ac_model='pvwatts')

    def set_parameter(self, date, weather=None, prices=None):
        self.date = date
        # set weather parameter for calculation
        self.weather = pd.DataFrame.from_dict(weather)
        self.weather['ghi'] = self.weather['dir'] + self.weather['dif']
        self.weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        self.weather.index = pd.date_range(start=date, periods=len(self.weather), freq='60min')
        # set prices
        self.prices = prices

    def optimize(self):
        self.photovoltaic.run_model(self.weather)
        # get generation in [MW]
        self.generation['powerSolar'] = np.clip(self.photovoltaic.ac.to_numpy(), self.maxPower)/10**6
        self.power = self.generation['powerSolar']

        return self.power


