import numpy as np
from sqlalchemy import create_engine
import pandas as pd
import time


class WeatherInterface:

    def __init__(self, user='opendata', password='opendata', database='weather', host='10.13.10.41', port=5432,
                 table='cosmo'):

        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        self.year_offset = 0

    def set_year_offset(self):
        self.year_offset = 0

    def get_temperature_in_area(self, area=50, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(temperature_avg) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}'" \
                    f"AND plz = {area} ;"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp, 'temp_air': value})
        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_wind_in_area(self, area=50, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(wind_meridional), avg(wind_zonal) FROM cosmo " \
                    f"WHERE timestamp = '{timestamp.isoformat()}'AND plz = {area} ;"
            res = self.engine.execute(query)
            values = res.fetchall()[0]
            wind_speed = (values[0] ** 2 + values[1] ** 2) ** 0.5
            direction = np.arctan2(values[0] / wind_speed, values[1] / wind_speed) * 180 / np.pi
            direction += 180
            direction = 90 - direction
            data_avg.append({'time': timestamp, 'wind_speed': wind_speed, 'direction': direction})

        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_direct_radiation_in_area(self, area=50, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(radiation_dir) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}'" \
                    f"AND plz = {area} ;"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp, 'dhi': value})
        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_diffuse_radiation_in_area(self, area=50, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(radiation_dif) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}'" \
                    f"AND plz = {area} ;"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp, 'dni': value})
        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_wind(self, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(wind_meridional), avg(wind_zonal) FROM cosmo " \
                    f"WHERE timestamp = '{timestamp.isoformat()}';"
            res = self.engine.execute(query)
            values = res.fetchall()[0]
            wind_speed = (values[0] ** 2 + values[1] ** 2) ** 0.5
            direction = np.arctan2(values[0] / wind_speed, values[1] / wind_speed) * 180 / np.pi
            direction += 180
            direction = 90 - direction
            data_avg.append({'time': timestamp, 'wind_speed': wind_speed, 'direction': direction})

        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_direct_radiation(self, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(radiation_dir) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}';"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp, 'dhi': value})
        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_diffuse_radiation(self, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(radiation_dif) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}';"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp, 'data': value})
        return pd.DataFrame(data_avg).set_index('dni', drop=True)

    def get_temperature(self, date=pd.to_datetime('1995-1-1')):
        data_avg = []
        for timestamp in pd.date_range(start=date, periods=24, freq='h'):
            query = f"SELECT avg(temperature_avg) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}';"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp, 'temp_air': value})
        return pd.DataFrame(data_avg).set_index('time', drop=True)


if __name__ == "__main__":

    interface_weather = WeatherInterface()

    temp = interface_weather.get_temperature_in_area(area=3)
    mean_temp = interface_weather.get_temperature()
    wind = interface_weather.get_wind_in_area(area=3)
    radR = interface_weather.get_direct_radiation_in_area(area=3)
    radF = interface_weather.get_diffuse_radiation_in_area(area=3)
    x = pd.concat([temp, wind], axis=1)
