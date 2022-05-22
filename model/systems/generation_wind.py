# third party modules
import os
import pandas as pd
import numpy as np
from windpowerlib import WindTurbine, ModelChain
from windpowerlib.wind_farm import WindFarm

# model modules
from systems.basic_system import EnergySystem
os.chdir(os.path.dirname(os.path.dirname(__file__)))


class WindModel(EnergySystem):

    def __init__(self, T, wind_turbine=None):
        super().__init__(T)

        if wind_turbine is None:
            wind_turbine = dict(turbine_type='E-82/2300', height=108, diameter=82)

        self.wind_turbine = None
        # TODO: Replace default with new data
        df = pd.read_csv(r'./systems/data/default_turbine.csv', sep=';', decimal=',')
        df['value'] /= max(df['value'])

        if isinstance(wind_turbine, list):
            wind_turbines, numbers = [], []
            heights = []
            for turbine in wind_turbine:
                diameter = min(turbine['diameter'], turbine['height']/2)
                height = max(turbine['diameter']*2, turbine['height'])
                max_power = turbine['maxPower']* 1e3 # [kW] -> [W]
                pow_c = df.copy()
                pow_c['value'] *= max_power
                w = {'hub_height': height,
                     'rotor_diameter': diameter,
                     'nominal_power': max_power,
                     'power_curve': pow_c}
                heights.append(w['hub_height'])
                wind_turbines.append(WindTurbine(**w))
                numbers.append(1)
            wind_turbine_fleet = pd.DataFrame({'wind_turbine': wind_turbines, 'number_of_turbines': numbers})

            efficiency = pd.DataFrame(data=dict(wind_speed=range(30), efficiency=[100 for _ in range(30)]))

            self.wind_turbine = WindFarm(wind_turbine_fleet, efficiency=efficiency)
            self.wind_turbine.hub_height = np.mean(heights)
            self.wind_turbine = self.wind_turbine.assign_power_curve()

        else:
            # windpowerlib uses Watt [W]
            diameter = min(wind_turbine['diameter'], wind_turbine['height']/2)
            height = max(wind_turbine['diameter']*2, wind_turbine['height'])
            max_power = wind_turbine['maxPower']* 1e3 # [kW] -> [W]
            pow_c = df.copy()
            pow_c['value'] *= max_power
            self.wind_turbine = WindTurbine(hub_height=height,
                                            rotor_diameter=diameter,
                                            nominal_power=max_power,
                                            power_curve=pow_c)

        self.mc = ModelChain(self.wind_turbine)

    def set_parameter(self, date, weather=None, prices=None):
        self.date = date

        index = pd.date_range(start=date, periods=self.T, freq='H')
        data = [0.2 * np.ones_like(self.t), weather['temp_air'], weather['wind_speed']]
        names = ['roughness_length', 'temperature', 'wind_speed']
        heights = [0, 2, 10]

        self.weather = pd.DataFrame(np.asarray(data).T, index=index, columns=[np.asarray(names), np.asarray(heights)])
        self.prices = prices

    def optimize(self):
        self.mc.run_model(self.weather)
        # windpowerlib calculated in [W]
        self.generation['wind'] = np.asarray(self.mc.power_output, dtype=np.float64)/ 1e3 # [W] -> [kW]
        self.power = self.generation['wind']

        return self.power
