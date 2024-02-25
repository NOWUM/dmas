import numpy as np
from sqlalchemy import create_engine, text
import pandas as pd


class WeatherInterface:

    def __init__(self, name, database_url):
        self.engine = create_engine(database_url, connect_args={"application_name": name})

    def get_param(self, param, date=pd.to_datetime('1995-01-01'), area=None):
        data_avg = []
        if param == 'wind_speed':
            return self.get_wind(date, area)
        with self.engine.begin() as connection:
            for timestamp in pd.date_range(start=date, periods=24, freq='h'):
                if timestamp < pd.to_datetime('2019-01-01'):
                    query = f"SELECT avg({param}) as {param} FROM cosmo WHERE time = '{timestamp.isoformat()}' "
                    if area is not None:
                        query += f" AND nuts LIKE '{area.upper()}%%' ;"
                else:
                    query = f"SELECT avg({param}) as {param} FROM ecmwf_eu WHERE time = '{timestamp.isoformat()}' "
                    if area is not None:
                        query += f" AND nuts_id LIKE '{area.upper()}%%' ;"

                res = connection.execute(text(query))
                value = res.fetchall()[0][0]
                data_avg.append({'time': timestamp, param: value})

        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_wind(self, date=pd.to_datetime('1995-01-01'), area=None):
        data_avg = []
        with self.engine.begin() as connection:
            for timestamp in pd.date_range(start=date, periods=24, freq='h'):
                if timestamp < pd.to_datetime('2019-01-01'):
                    query = f"SELECT avg(wind_meridional), avg(wind_zonal) FROM cosmo WHERE time = '{timestamp.isoformat()}'"
                    if area is not None:
                        query += f" AND nuts LIKE '{area.upper()}%%' ;"
                    res = connection.execute(text(query))
                    values = res.fetchall()[0]
                    wind_speed = (values[0] ** 2 + values[1] ** 2) ** 0.5
                else:
                    query = f"SELECT avg(wind_speed) FROM ecmwf_eu WHERE time = '{timestamp.isoformat()}' "
                    if area is not None:
                        query += f" AND nuts_id LIKE '{area.upper()}%%' ;"
                    res = connection.execute(text(query))
                    wind_speed = res.fetchall()[0]

                data_avg.append({'time': timestamp, 'wind_speed': wind_speed})
        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_direct_radiation(self, date=pd.to_datetime('1995-01-01'), area=None):
        return self.get_param('dhi', date, area)

    def get_diffuse_radiation(self, date=pd.to_datetime('1995-01-01'), area=None):
        return self.get_param('dni', date, area)

    def get_temperature(self, date=pd.to_datetime('1995-01-01'), area=None):
        return self.get_param('temp_air', date, area)


if __name__ == "__main__":
    weather_database_uri = 'postgresql://readonly:readonly@10.13.10.41:5432/weather'
    interface_weather = WeatherInterface('test', weather_database_uri)

    date = pd.to_datetime('2020-01-01')
    mean_temp_cosmo = interface_weather.get_temperature()
    mean_temp_ecmwf = interface_weather.get_temperature(date)
    wind_cosmo = interface_weather.get_wind(area='DE111')
    wind_ecmwf = interface_weather.get_wind(area='DE111', date=date)
    radR = interface_weather.get_direct_radiation(area='DE111')
    radF = interface_weather.get_diffuse_radiation(area='DE111')
    x = pd.concat([mean_temp_cosmo, wind_cosmo], axis=1)
