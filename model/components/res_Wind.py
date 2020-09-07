import os
from scipy import stats
import numpy as np
from components.basic_EnergySystem import energySystem
from windpowerlib import WindTurbine, power_output, wind_speed, wind_turbine, temperature, ModelChain
import pandas as pd
import numpy as np


class wind_model(energySystem):

    def __init__(self, turbine_type, hub_height=112, rotor_diameter=102,
                 t=np.arange(24), T=24, dt=1, power_curve=None):

        super().__init__(t, T, dt)

        self.default_turbine_type = 'E-82/2300'
        self.diameter = rotor_diameter
        self.hub_height = hub_height

        if power_curve is None:

            if turbine_type in wind_turbine.get_turbine_types(print_out=False)['turbine_type'].unique():
                self.windTurbine = WindTurbine(hub_height=self.hub_height, turbine_type=turbine_type)
            elif turbine_type in ['AN/1000', 'AN/1300', 'E-66/1500']:
                df = pd.read_excel(r'./data/Ref_TurbineData.xlsx', sheet_name=turbine_type.replace('/', ' '))
                self.windTurbine = WindTurbine(hub_height=self.hub_height, rotor_diameter=rotor_diameter,
                                               power_curve=df)
            else:
                self.windTurbine = WindTurbine(hub_height=self.hub_height, turbine_type=self.default_turbine_type)

        else:
            pass

        self.modelchain_data = {

            'wind_speed_model': 'hellman',
            'density_model': 'barometric',
            'temperature_model': 'linear_gradient',
            'power_output_model': 'power_curve',
            'density_correction': False,
            'obstacle_height': None,
            'hellman_exp': 0.125}

        self.mc = ModelChain(self.windTurbine)

    def build(self, data, ts, date):
        index = pd.date_range(start=date, periods=self.T, freq='H')
        roughness = 0.2 * np.ones_like(self.t)
        weather_df = pd.DataFrame(np.asarray([roughness, ts['wind'], ts['temp']]).reshape((self.T, -1)),
                                  index=index,
                                  columns=[np.asarray(['roughness_length', 'wind_speed', 'temperature']),
                                           np.asarray([0, 10, 2])])

        self.mc.run_model(weather_df)
        powerResult = np.asarray(self.mc.power_output, dtype=np.float64)

        self.generation['wind'] = np.nan_to_num(powerResult)
        self.power = np.nan_to_num(powerResult)


if __name__=="__main__":

    from scipy import interpolate
    import matplotlib.pyplot as plt

    ws = np.array([])
    powerCurves = []

    a1 = wind_model(turbine_type='AD116/5000')
    ws = np.concatenate((a1.windTurbine.power_curve['wind_speed'].to_numpy(), ws))
    powerCurves.append((a1.windTurbine.power_curve['wind_speed'].to_numpy(),
                        a1.windTurbine.power_curve['value'].to_numpy()))

    a2 = wind_model(turbine_type='E-101/3500')
    ws = np.concatenate((a2.windTurbine.power_curve['wind_speed'].to_numpy(), ws))
    powerCurves.append((a2.windTurbine.power_curve['wind_speed'].to_numpy(),
                        a2.windTurbine.power_curve['value'].to_numpy()))

    a3 = wind_model(turbine_type='E-82/2000')
    ws = np.concatenate((a3.windTurbine.power_curve['wind_speed'].to_numpy(), ws))
    powerCurves.append((a3.windTurbine.power_curve['wind_speed'].to_numpy(),
                        a3.windTurbine.power_curve['value'].to_numpy()))

    a4 = wind_model(turbine_type='E-66/1500')
    ws = np.concatenate((a4.windTurbine.power_curve['wind_speed'].to_numpy(), ws))
    powerCurves.append((a4.windTurbine.power_curve['wind_speed'].to_numpy(),
                        a4.windTurbine.power_curve['value'].to_numpy()))

    a5 = wind_model(turbine_type='blablabla')
    ws = np.concatenate((a5.windTurbine.power_curve['wind_speed'].to_numpy(), ws))
    powerCurves.append((a5.windTurbine.power_curve['wind_speed'].to_numpy(),
                        a5.windTurbine.power_curve['value'].to_numpy()))


    # a5.build(date='2019-01-01', data={}, ts={})

    ws = np.sort(np.unique(ws))
    value = np.zeros_like(ws)
    legend = []
    counter = 1
    for powerCurve in powerCurves:
        f = interpolate.interp1d(powerCurve[0], powerCurve[1], fill_value=0, bounds_error=False)
        plt.plot(powerCurve[0], powerCurve[1])
        value += f(ws)
        legend.append(counter)
        counter += 1

    totalCurve = (ws, value)
    plt.plot(ws, value)
    legend.append('total')
    plt.legend(legend)