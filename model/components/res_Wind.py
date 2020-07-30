import os
from scipy import stats
import numpy as np
from components.basic_EnergySystem import energySystem
from windpowerlib import WindTurbine, power_output, wind_speed
from windpowerlib import temperature
from windpowerlib import wind_turbine as wt
import pandas as pd
import numpy as np


class wind_model(energySystem):

    def __init__(self, turbine_type, hub_height=112, rotor_diameter=102,
                 t=np.arange(24), T=24, dt=1,
                 nominal_power_4turbine_without_power_curve=None):
        """
        inits a wind model
        :param turbine_type: name of turbine_type corresponding to an entry in windpowerlib.wind_turbine.get_turbine_types
         or to an folder with a 'power_curve.csv' file in path 'data/windModel/'
        :param hub_height: height of windturbine's hub in [m]. Default: 100 [m]
        :param rotor_diameter: in [m]
        :param t: Metainfo Zeit (see class energySystem in basic_EbergySystem.py)
        :param T: Metainfo Zeit (see class energySystem in basic_EbergySystem.py)
        :param dt: Metainfo Zeit (see class energySystem in basic_EbergySystem.py)
        :param nominal_power_4turbine_without_power_curve: nominal power of wind turbine. This value is just used, if
                                                           there is no power curve available 4 the turbine_type

        self.generation['wind'] and self.power is in [MW]
        """

        super().__init__(t, T, dt)

        self.__windTurbine = None
        self.hub_height = hub_height

        self.__nominal_power_4turbine_without_power_curve = None

        self.default_turbine_type = 'E-82/2300'
        self.__nominal_power_of_default_turbine = float(self.default_turbine_type.split('/')[1])
        self.weibullWind = np.load(r'./data/Ref_Wind.array')
        df_lib_turbine_types = wt.get_turbine_types(print_out=False)

        if turbine_type in df_lib_turbine_types['turbine_type'].unique():
            self.__windTurbine = WindTurbine(hub_height=self.hub_height,
                                             turbine_type=turbine_type)
        else:
            try:
                path = os.getcwd() + os.sep + 'data' + os.sep + 'windModel'

                # https://windpowerlib.readthedocs.io/en/stable/temp/windpowerlib.wind_turbine.WindTurbine.html#windpowerlib.wind_turbine.WindTurbine


                self.__windTurbine = WindTurbine(hub_height=self.hub_height,
                                                 # power_curve=                     # opt.
                                                 # power_coefficient_curve=None,    # opt.
                                                 turbine_type=turbine_type,         # opt.
                                                 rotor_diameter=rotor_diameter,
                                                 # nominal_power                    # opt.
                                                 path=path
                                                 )

                #if is needed to check if there is a power_curve (or if there are data 4 the turbine_type)
                if (self.__windTurbine.power_curve['value'] is None):
                    # just doing anything to call the if-statement. If the statement fails,
                    # the default_turbine is taken (see exception)
                    pass

            except Exception as e:
                # default turbine typ E-82 by Enercon

                # if power scale is needed, self.__nominal_power_4turbine_without_power_curve is set
                if(not(nominal_power_4turbine_without_power_curve == 0
                       or nominal_power_4turbine_without_power_curve == self.__nominal_power_of_default_turbine
                       or nominal_power_4turbine_without_power_curve is None)):
                    self.__nominal_power_4turbine_without_power_curve = nominal_power_4turbine_without_power_curve

                print('\n\033[31m' + 'Turbine type ' + str(turbine_type) + ' not found.' + '\033[0m')
                print('The default turbine type ' + str(self.default_turbine_type) + ' is used.')

                self.__windTurbine = WindTurbine(hub_height=self.hub_height,
                                                 turbine_type=self.default_turbine_type)


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

        randomWind = []
        interval = np.linspace(2, 9, 8)

        for ws in ts['wind']:
            for i in range(1, 8):
                if interval[i - 1] < ws <= interval[i]:
                    randomWind.append(stats.exponweib.rvs(*self.weibullWind[i]))
                    break
            if ws > 9:
                randomWind.append(stats.exponweib.rvs(*self.weibullWind[8]))
            elif ws < 2:
                randomWind.append(stats.exponweib.rvs(*self.weibullWind[0]))

        wind = wind_speed.hellman(wind_speed=np.asarray(randomWind, dtype=np.float64),
                                  wind_speed_height=10.,
                                  hub_height=self.hub_height,
                                  roughness_length=None,
                                  hellman_exponent=0.125)

        powerResult = power_output.power_curve(wind_speed=wind,
                                               power_curve_wind_speeds=self.__windTurbine.power_curve['wind_speed'],
                                               power_curve_values=self.__windTurbine.power_curve['value'],
                                               # density=densityInKgQm,
                                               density_correction=False)

        # change result from [W] to [MW]
        powerResult = np.asarray(([x/(10**6) for x in powerResult]), dtype=np.float64)

        if(self.__nominal_power_4turbine_without_power_curve is not None):
            # if the default turbine is taken and the nominal_power_4turbine_without_power_curve is !=0 or
            # != the nominal power of the default_turbine (act.: 2300 W), the power result is scaled
            powerResult = powerResult * (self.__nominal_power_4turbine_without_power_curve / self.__nominal_power_of_default_turbine)

        self.generation['wind'] = powerResult
        self.power = powerResult
