# third party modules
import pandas as pd
import numpy as np
from windpowerlib import WindTurbine, ModelChain
from windpowerlib.wind_farm import WindFarm

# model modules
from systems.basic_system import EnergySystem

# TODO: Replace default with new data
default_power_curve = pd.read_csv(r'./systems/data/default_turbine.csv', sep=';', decimal=',')
default_power_curve['value'] /= max(default_power_curve['value'])

def create_windturbine(turbine):
    if not turbine['height'] > 0:
        turbine['height'] = turbine['maxPower']/20

    # small wind systems have no diameter set
    # causing nan values in generation and therefore in the whole portfolio
    if not turbine['diameter'] > 0:
        turbine['diameter'] = turbine['height']

    diameter = min(turbine['diameter'], turbine['height']*2-1)
    height = max(turbine['diameter']/2+1, turbine['height'])
    max_power = turbine['maxPower']* 1e3 # [kW] -> [W]
    pow_c = default_power_curve.copy()
    pow_c['value'] *= max_power
    return WindTurbine(hub_height=height,
            rotor_diameter=diameter,
            nominal_power=max_power,
            power_curve=pow_c)

class WindModel(EnergySystem):

    def __init__(self, T, wind_turbine=None):
        super().__init__(T)

        if wind_turbine is None:
            wind_turbine = dict(turbine_type='E-82/2300', height=108, diameter=82)

        self.wind_turbine = None

        if isinstance(wind_turbine, list):
            wind_turbines, numbers = [], []
            for turbine in wind_turbine:
                wind_turbines.append(create_windturbine(turbine))
                numbers.append(1)
            wind_turbine_fleet = pd.DataFrame({'wind_turbine': wind_turbines, 'number_of_turbines': numbers})

            efficiency = pd.DataFrame(data=dict(wind_speed=range(30), efficiency=[1 for _ in range(30)]))

            self.wind_turbine = WindFarm(wind_turbine_fleet, efficiency=efficiency)

            # calculates and sets the mean_hub_height
            # Hub heights of wind turbines with
            # higher nominal power weigh more than others.
            self.wind_turbine.mean_hub_height()
            # calculates a smoothed average power curve
            # and assigns it to the windpark
            self.wind_turbine = self.wind_turbine.assign_power_curve()

        else:
            self.wind_turbine = create_windturbine(wind_turbine)

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
