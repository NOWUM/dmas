from sqlalchemy import create_engine
import pandas as pd
import time


class Weather:

    def __init__(self, user='opendata', password='opendata', database='weather', host='10.13.10.41', port=5432,
                 table='cosmo'):

        self.engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    def get_temperature_in_area(self, area=50, timestamps=[]):
        # TODO: Check Performance during the Simulation
        # query = f"SELECT timestamp, avg(temperature_avg) FROM cosmo WHERE " \
        #         f"timestamp >= '{timestamps[0].isoformat()}' AND timestamp < '{timestamps[-1].isoformat()}'" \
        #         f"AND plz = {area} GROUP BY timestamp;"
        # df = pd.read_sql(query, con=self.engine)
        data_avg = []
        for timestamp in timestamps:
            query = f"SELECT avg(temperature_avg) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}'" \
                    f"AND plz = {area} ;"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp,
                                    'temperature_avg': value})

        return pd.DataFrame(data_avg).set_index('time', drop=True)

        # return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_wind_in_area(self, area=50, timestamps=[]):
        data_avg = []
        for timestamp in timestamps:
            query = f"SELECT avg(wind_meridional), avg(wind_zonal) FROM cosmo " \
                    f"WHERE timestamp = '{timestamp.isoformat()}'AND plz = {area} ;"
            res = self.engine.execute(query)
            values = res.fetchall()[0]
            data_avg.append({'time': timestamp,
                             'wind_avg': (values[0]**2 + values[1]**2)**0.5})

        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_direct_radiation_in_area(self, area=50, timestamps=[]):
        data_avg = []
        for timestamp in timestamps:
            query = f"SELECT avg(radiation_dir) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}'" \
                    f"AND plz = {area} ;"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp,
                                    'radiation_dir': value})

        return pd.DataFrame(data_avg).set_index('time', drop=True)

    def get_diffuse_radiation_in_area(self, area=50, timestamps=[]):
        data_avg = []
        for timestamp in timestamps:
            query = f"SELECT avg(radiation_dif) FROM cosmo WHERE timestamp = '{timestamp.isoformat()}'" \
                    f"AND plz = {area} ;"
            res = self.engine.execute(query)
            value = res.fetchall()[0][0]
            data_avg.append({'time': timestamp,
                                    'radiation_dif': value})

        return pd.DataFrame(data_avg).set_index('time', drop=True)


if __name__ == "__main__":

    interface_weather = Weather()
    hour_range = list(pd.date_range(start='1995-01-01 00:00', end='1995-01-01 23:00', freq='h'))
    temp = interface_weather.get_temperature_in_area(area=3, timestamps=hour_range)
    wind = interface_weather.get_wind_in_area(area=3, timestamps=hour_range)
    radR = interface_weather.get_direct_radiation_in_area(area=3, timestamps=hour_range)
    radF = interface_weather.get_diffuse_radiation_in_area(area=3, timestamps=hour_range)
