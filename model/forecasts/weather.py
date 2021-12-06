from forecasts.basic_forecast import BasicForecast
import numpy as np


class WeatherForecast(BasicForecast):

    def __init__(self):
        super().__init__()
        self.mean_weather = {}

    def forecast(self, date):
        random_factor = np.random.uniform(low=0.95, high=1.05)
        temperature = self.weather_database.get_temperature(date) * random_factor
        wind = self.weather_database.get_wind(date) * random_factor
        direct_radiation = self.weather_database.get_direct_radiation(date) * random_factor
        diffuse_radiation = self.weather_database.get_diffuse_radiation(date) * random_factor

        return temperature, wind, direct_radiation + diffuse_radiation

    def forecast_for_area(self, date, area):
        random_factor = np.random.uniform(low=0.95, high=1.05)
        temperature = self.weather_database.get_temperature_in_area(date, area) * random_factor
        wind = self.weather_database.get_wind_in_area(date, area) * random_factor
        direct_radiation = self.weather_database.get_direct_radiation_in_area(date, area) * random_factor
        diffuse_radiation = self.weather_database.get_diffuse_radiation_in_area(date, area) * random_factor

        return temperature, wind, direct_radiation, diffuse_radiation

    def collect_data(self, date):
        pass

    def fit_model(self):
        pass
