import pandas as pd
import numpy as np
from pvlib.location import Location
from datetime import timedelta as td


class WeatherForecast:

    def __init__(self, position, weather_interface):
        self.weather = weather_interface
        self.position = position
        self.location = Location(longitude=position['lon'], latitude=position['lat'])
        self.sun_position = self.location.get_solarposition(pd.date_range(start='1972-01-01 00:00', periods=8684,
                                                                          freq='h'))
        self._weather_params = ['temp_air', 'wind_meridional', 'wind_zonal', 'dhi', 'dni']
        self._last_weather = pd.DataFrame()

    def _get_weather_data(self, date_range: pd.DatetimeIndex, local: str = None):
        data = {param: [] for param in self._weather_params}
        if local is not None:
            data['azimuth'] = []
            data['zenith'] = []

        for date in date_range:
            for param in self._weather_params:
                if local is not None:
                    df = self.weather.get_param_in_area(param=param, date=date, area=local)
                else:
                    df = self.weather.get_param(param=param, date=date)
                data[param] += list(df.values.flatten())
            if local:
                index = self.sun_position.index.day_of_year == date.day_of_year
                data['azimuth'] += list(self.sun_position.loc[index, 'azimuth'].values.flatten())
                data['zenith'] += list(self.sun_position.loc[index, 'zenith'].values.flatten())

        data = pd.DataFrame.from_dict(data)

        data['wind_speed'] = (data['wind_meridional'].values ** 2 + data['wind_zonal'].values ** 2) ** 0.5
        data['ghi'] = data['dhi'] + data['dni']

        del data['wind_meridional']
        del data['wind_zonal']

        return data

    def forecast(self, date: pd.Timestamp, steps: int = 24, local: str = None):
        if (steps % 24) > 0:
            print(f'wrong step size: {steps}')
            steps -= steps % 24
            print(f'set step size to {steps}')
        steps = max(steps, 24)
        range_ = pd.date_range(start=date, end=date + td(days=(steps//24) - 1), freq='d')
        try:
            weather = self._get_weather_data(range_, local)
            self._last_weather = weather.iloc[:24, :].copy()
        except Exception as e:
            print('ERROR getting WeatherForecast - using last weather')
            # use last weather on exception (incomplete data in database)
            weather = self._last_weather

        for column in weather.columns:
            if column not in ['azimuth', 'zenith']:
                weather[column] *= np.random.uniform(low=0.95, high=1.05, size=len(weather[column]))
        return weather

    def get_last(self):
        return self._last_weather
