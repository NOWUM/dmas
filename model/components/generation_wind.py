# third party modules
import os
import pandas as pd
import numpy as np
from windpowerlib import WindTurbine, ModelChain, wind_turbine as wt


# model modules
from components.energy_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class WindModel(EnergySystem):

    def __init__(self, t=np.arange(24), T=24, dt=1, wind_turbine=None, power_curve=None):
        super().__init__(t, T, dt)

        if wind_turbine is None:
            wind_turbine = dict(turbine_type='E-82/2300', height=112, diameter=102)
        self.wind_turbine = wind_turbine

        self.default_turbine_type = 'E-82/2300'

        if power_curve is None:

            if wind_turbine['turbine_type'] in wt.get_turbine_types(print_out=False)['turbine_type'].unique():
                self.wind_turbine = WindTurbine(hub_height=self.wind_turbine['height'],
                                                rotor_diameter=self.wind_turbine['diameter'],
                                                turbine_type=self.wind_turbine['turbine_type'])
            elif wind_turbine['turbine_type'] in ['AN/1000', 'AN/1300', 'E-66/1500']:
                df = pd.read_excel(r'./data/Ref_TurbineData.xlsx', sheet_name=wind_turbine['turbine_type'].replace('/', ' '))
                self.wind_turbine = WindTurbine(hub_height=self.wind_turbine['height'],
                                                rotor_diameter=self.wind_turbine['diameter'],
                                                power_curve=df)
            else:
                self.wind_turbine = WindTurbine(hub_height=self.wind_turbine['height'],
                                                rotor_diameter=self.wind_turbine['diameter'],
                                                turbine_type=self.default_turbine_type)

        else:
            self.wind_turbine = WindTurbine(hub_height=self.wind_turbine['height'],
                                            rotor_diameter=wind_turbine['diameter'], power_curve=power_curve)

        self.mc = ModelChain(self.wind_turbine)

    def set_parameter(self, date, weather=None, prices=None):
        self.date = pd.to_datetime(date)
        # set weather parameter for calculation
        index = pd.date_range(start=self.date, periods=self.T, freq='H')
        roughness = 0.2 * np.ones_like(self.t)
        self.weather = pd.DataFrame(np.asarray([roughness, weather['wind'], weather['temp']]).T,
                                    index=index,
                                    columns=[np.asarray(['roughness_length', 'wind_speed', 'temperature']),
                                             np.asarray([0, 10, 2])])
        # set prices
        self.prices = prices

    def optimize(self):
        self.mc.run_model(self.weather)

        power_wind = np.asarray(self.mc.power_output, dtype=np.float64)

        self.generation['powerWind'] = np.nan_to_num(power_wind)/10**6
        self.power = np.nan_to_num(power_wind)/10**6

        return self.power


