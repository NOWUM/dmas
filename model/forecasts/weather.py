from datetime import timedelta as td

import numpy as np
import pandas as pd
from pvlib.irradiance import complete_irradiance, erbs
from pvlib.location import Location
from interfaces.weather import WeatherInterface
from collections import defaultdict


WEATHER_PARAMS_COSMO = ['temp_air', 'wind_speed', 'dhi', 'dni']
WEATHER_PARAMS_ECMWF = ['temp_air', 'wind_speed', 'ghi']  # 'direction' not needed


class WeatherForecast:

    def __init__(self, position, weather_interface):
        self.weather = weather_interface
        self.position = position
        self.location = Location(longitude=position['lon'], latitude=position['lat'], tz='Europe/Berlin')
        self.sun_position = self.location.get_solarposition(pd.date_range(start='1972-01-01 00:00', periods=8684,
                                                                          freq='h'))
        self._previous_weather = pd.DataFrame()

    def _get_weather_data(self, date_range: pd.DatetimeIndex, local: str = None):
        data = defaultdict(lambda: [])

        for date in date_range:
            index = self.sun_position.index.day_of_year == date.day_of_year
            data['azimuth'] += list(self.sun_position.loc[index, 'azimuth'].values.flatten())
            data['zenith'] += list(self.sun_position.loc[index, 'zenith'].values.flatten())

            if date < pd.to_datetime('2019-01-01'):
                for param in WEATHER_PARAMS_COSMO:
                    df = self.weather.get_param(param=param, date=date, area=local)
                    data[param] += list(df[param].values.flatten())

                # COSMO does not contain ghi which is needed for pvlib
                calculated = complete_irradiance(np.array(data['zenith']), np.array(data['dhi']), np.array(data['dni']))
                ghi = calculated['ghi'].fillna(0)
                data['ghi'] = list(ghi)
            else:

                for param in WEATHER_PARAMS_ECMWF:
                    df = self.weather.get_param(param=param, date=date, area=local)
                    data[param] += list(df[param].values.flatten())

                # ECMWF does not contain dni and dhi which must be calculated
                calculated = erbs(np.array(data['ghi']), np.array(data['zenith']), date.day_of_year)

                dni = np.nan_to_num(calculated['dni'])
                dhi = np.nan_to_num(calculated['dhi'])
                data['dni'] = list(dni)
                data['dhi'] = list(dhi)

        return pd.DataFrame.from_dict(data)

    def forecast(self, date: pd.Timestamp, steps: int = 24, local: str = None):
        if (steps % 24) > 0:
            print(f'wrong step size: {steps}')
            steps -= steps % 24
            print(f'set step size to {steps}')
        steps = max(steps, 24)
        range_ = pd.date_range(start=date, end=date + td(days=(steps//24) - 1), freq='d')
        try:
            weather = self._get_weather_data(range_, local)
            self._previous_weather = weather.iloc[:24, :].copy()
        except Exception as e:
            print(f'ERROR getting WeatherForecast {repr(e)} - using previous weather')
            # use last weather on exception (incomplete data in database)
            weather = self._previous_weather

        for column in weather.columns:
            if column not in ['azimuth', 'zenith']:
                weather[column] *= np.random.uniform(low=0.95, high=1.05, size=len(weather[column]))
        return weather

    def get_last(self):
        return self._previous_weather


if __name__ == '__main__':
    position = dict(lon=50.8, lat=6.12)
    weather_database_uri = 'postgresql://readonly:readonly@10.13.10.41:5432/weather'
    interface_weather = WeatherInterface('test', weather_database_uri)
    area = 'DE111'

    wf = WeatherForecast(position, interface_weather)
    date = pd.to_datetime('2010-07-01')
    date_range = pd.date_range(start=date, end=date)
    # cosmo
    weather = wf._get_weather_data(date_range, area)
    weather_f = wf.forecast(date, local=area)

    # ecmwf
    date = pd.to_datetime('2020-07-01')
    date_range = pd.date_range(start=date, end=date)
    weather_ecmwf = wf._get_weather_data(date_range, area)
    weather_ecmwf_f = wf.forecast(date, local=area)
