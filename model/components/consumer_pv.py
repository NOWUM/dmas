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
from apps.slpP import slpGen as slpP
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class PvModel(es):

    def __init__(self, t=np.arange(24), T=24, dt=1, e_el=3000, photovoltaic=None,
                 lat=50.77, lon=6.09):
        super().__init__(t, T, dt)

        if photovoltaic is None:
            photovoltaic = dict(maxPower=2.9, azimuth=180, tilt=30)

        # initialize weather for generation calculation
        self.weather = None

        # initialize standard h0 consumer attributes
        self.e_el = e_el
        self.slpP = slpP(refSLP=np.asarray(np.load(open(r'./data/Ref_H0.array', 'rb')), np.float32))

        # add photovoltaic system
        pv_system = PVSystem(module_parameters=dict(pdc0=1000 * photovoltaic['maxPower'], gamma_pdc=-0.004),
                             inverter_parameters=dict(pdc0=1000 * photovoltaic['maxPower']),
                             surface_tilt=photovoltaic['tilt'], surface_azimuth=photovoltaic['azimuth'], albedo=0.25,
                             temperature_model_parameters=TEMPERATURE_MODEL_PARAMETERS['pvsyst']['insulated'],
                             losses_parameters=dict(availability=0, lid=0, shading=1, soiling=1))

        # aggregate in model chain
        self.photovoltaic = ModelChain(pv_system, Location(lat, lon), aoi_model='physical', spectral_model='no_loss',
                                       temperature_model='pvsyst', losses_model='pvwatts', ac_model='pvwatts')

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        # set weather parameter for calculation
        self.weather = pd.DataFrame.from_dict(weather)
        self.weather['ghi'] = self.weather['dir'] + self.weather['dif']
        self.weather.columns = ['wind_speed', 'dni', 'dhi', 'temp_air', 'ghi']
        self.weather.index = pd.date_range(start=date, periods=len(self.weather), freq='60min')
        # set prices
        self.prices = prices

    def optimize(self):
        self.photovoltaic.run_model(self.weather)
        # get generation in [kW]
        self.generation['powerSolar'] = self.photovoltaic.ac.to_numpy()/1000
        # get demand in [kW]
        demand = self.slpP.get_profile(self.date.dayofyear, self.date.dayofweek, self.e_el).reshape((96, 1))
        self.demand['power'] = np.asarray([np.mean(demand[i:i + 3]) for i in range(0, 96, 4)], np.float).reshape((-1,))

        # get grid usage in [kW]
        grid_use = self.demand['power'] - self.generation['powerSolar']
        # gird usage in [kW]
        self.power = np.asarray(grid_use, np.float).reshape((-1,))
        return self.power


if __name__ == "__main__":

    consumer = PvModel()

