import os

import numpy as np
from components.basic_EnergySystem import energySystem
from windpowerlib import WindTurbine, power_output
from windpowerlib import temperature
from windpowerlib import wind_turbine as wt
import pandas as pd

class wind_model(energySystem):

    def __init__(self, turbine_type, hub_height=112, rotor_diameter=102, t=np.arange(24), T=24, dt=1):
        """
        inits a wind model
        :param turbine_type: name of turbine_type corresponding to an entry in windpowerlib.wind_turbine.get_turbine_types
         or to an folder with a 'power_curve.csv' file in path 'data/windModel/'
        :param hub_height: height of windturbine's hub in [m]. Default: 100 [m]
        :param rotor_diameter: in [m]
        :param t: Metainfo Zeit (see class energySystem in basic_EbergySystem.py)
        :param T: Metainfo Zeit (see class energySystem in basic_EbergySystem.py)
        :param dt: Metainfo Zeit (see class energySystem in basic_EbergySystem.py)
        """

        super().__init__(t, T, dt)

        self.__windTurbine = None

        df_lib_turbine_types = wt.get_turbine_types(print_out=False)

        if turbine_type in df_lib_turbine_types['turbine_type'].unique():
            self.__windTurbine = WindTurbine(hub_height=100 if (hub_height is None) else hub_height,
                                             turbine_type=turbine_type)
        else:
            try:
                path = os.getcwd() + os.sep + 'data' + os.sep + 'windModel'

                # https://windpowerlib.readthedocs.io/en/stable/temp/windpowerlib.wind_turbine.WindTurbine.html#windpowerlib.wind_turbine.WindTurbine
                self.__windTurbine = WindTurbine(hub_height=100 if (hub_height is None) else hub_height,
                                                 # power_curve=                     # opt.
                                                 # power_coefficient_curve=None,    # opt.
                                                 turbine_type=turbine_type,         # opt.
                                                 rotor_diameter=rotor_diameter,
                                                 # nominal_power                    # opt.
                                                 path=path
                                                 )

                if (self.__windTurbine.power_curve['value'] is None):
                    print('Wird nicht aufgerufen, die if Abfrage ist wichtig, da Python dann merkt ob eine power curve verfuegbar ist.')
                    #raise Exception('Turbine type ' + turbine_type + ' not found.')

            except Exception as e:
                # default turbine typ E-82 by Enercon TODO
                default_turbine_type = 'E-82/2300'
                print('\n\033[31m' + 'Error Message:\n' + str(e) + '\n' + '\033[0m')
                print ('The default turbine type ' + str(default_turbine_type) + ' is used.')
                self.__windTurbine = WindTurbine(hub_height=100 if (hub_height is None) else hub_height,
                                                 turbine_type=default_turbine_type)

    def build(self, data, ts, date):

        """
        # not used:
        #----------

        tempK = temperature.linear_gradient(temperature=pd.Series([x+273.15 for x in ts['TAmb']]),  # Temp from [C] in [K]
                                            temperature_height=2,  # Temp Messung auf 2m
                                            hub_height=self.__windTurbine.hub_height )

        densityInKgQm = density.barometric(pressure=ts['density'], # [Pa]
                                           pressure_height=2,      # Messung auf 2m 
                                           hub_height=wt.hub_height,
                                           temperature_hub_height=tempK)
        """

        powerResult = power_output.power_curve(wind_speed=ts['wind'],
                                               power_curve_wind_speeds=self.__windTurbine.power_curve['wind_speed'],
                                               power_curve_values=self.__windTurbine.power_curve['value'],
                                               # density=densityInKgQm,
                                               density_correction=False)

        # format: {24,}
        self.generation['wind'] = powerResult
        self.power = powerResult
