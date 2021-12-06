# third party modules
import os
import pandas as pd
import numpy as np
from windpowerlib import WindTurbine, ModelChain, wind_turbine as wt
from windpowerlib import wind_farm

# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class WindModel(EnergySystem):

    def __init__(self, T, wind_turbine=None):
        super().__init__(T)

        if wind_turbine is None:
            wind_turbine = dict(turbine_type='E-82/2300', height=112, diameter=102)

        self.wind_turbine = None
        # TODO: Replace default with new data
        df = pd.read_csv(r'./data/default_turbine.csv', sep=';', decimal=',')

        if isinstance(wind_turbine, list):
            wind_turbines = {}

            for turbine in wind_turbines:
                wind_turbines.update({turbine['unitID']: {'hub_height': turbine['height'],
                                                          'rotor_diameter': turbine['diameter'],
                                                          'power_curve': df}})

            wind_turbine_fleet = pd.DataFrame({'wind_turbine': [value for _, value in wind_turbines.items()],
                                               'number_of_turbines': [1 for _, _ in wind_turbines.items()]})

            self.wind_turbine = wind_farm.WindFarm(wind_turbine_fleet)

        else:
            self.wind_turbine = WindTurbine(hub_height=wind_turbine['height'],
                                            rotor_diameter=wind_turbine['diameter'],
                                            power_curve=df)

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

        self.generation['wind'] = np.nan_to_num(power_wind)/10**6
        self.power = np.nan_to_num(power_wind)/10**6

        return self.power
