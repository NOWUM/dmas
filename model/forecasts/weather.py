import pandas as pd
import numpy as np

from forecasts.basic_forecast import BasicForecast


class WeatherForecast(BasicForecast):

    def __init__(self, position):
        super().__init__(position)
        self.sun_position = self.location.get_solarposition(pd.date_range(start='1972-01-01 00:00', periods=8684,
                                                                          freq='h'))

    def forecast(self, date):
        random_factor = np.random.uniform(low=0.95, high=1.05)
        temp_air = self.weather.get_temperature(date) * random_factor
        wind_speed = self.weather.get_wind(date) * random_factor
        dhi = self.weather.get_direct_radiation(date) * random_factor
        dni = self.weather.get_diffuse_radiation(date) * random_factor
        df = pd.concat([temp_air, wind_speed, dhi, dni], axis=1)
        df['ghi'] = df['dhi'] + df['dni']
        return df

    def forecast_for_area(self, date, area):

        azimuth = self.sun_position.loc[self.sun_position.index.day_of_year == date.day_of_year, 'azimuth'].to_numpy()
        zenith = self.sun_position.loc[self.sun_position.index.day_of_year == date.day_of_year, 'zenith'].to_numpy()

        random_factor = np.random.uniform(low=0.95, high=1.05)
        temp_air = self.weather.get_temperature_in_area(area, date) * random_factor
        wind_speed = self.weather.get_wind_in_area(area, date) * random_factor
        dhi = self.weather.get_direct_radiation_in_area(area, date) * random_factor
        dni = self.weather.get_diffuse_radiation_in_area(area, date) * random_factor
        df = pd.concat([temp_air, wind_speed, dhi, dni], axis=1)
        df['ghi'] = df['dhi'] + df['dni']
        df['azimuth'] = azimuth
        df['zenith'] = zenith

        return df

    def collect_data(self, date):
        pass

    def fit_model(self):
        pass
