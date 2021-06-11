# third party modules
import numpy as np
import pandas as pd
from influxdb import DataFrameClient
import multiprocessing
from joblib import Parallel, delayed
import time as tme


class WeatherGenerator:

    def __init__(self, host='149.201.88.150', port=8086, user='root', password='root', database='MAS2020_12'):

        # connection/interface to influx database
        self.influx = DataFrameClient(host=host, port=port, username=user, password=password, database=database)
        # Meta data
        self.database = database            # database name for simulation
        self.host = host                    # ip where influx is running
        self.port = port                    # port where influx is running
        self.user = user                    # login user
        self.password = password            # login password
        # year switch
        self.last_sim_year = 2018
        self.hist_year = 2018
        # current areas with hash values as position/index
        self.map_hash = pd.read_csv(r'./data/Ref_GeoInfo.csv', index_col=0, sep=';', decimal=',')
        self.map_hash = self.map_hash.set_index('hash')

    def generate_weather(self, valid, date):

        self.influx.switch_database('weatherData')
        if valid:

            query = 'select * from "DWD_REA6" where time >= \'%s\' and time < \'%s\'' \
                    % (date.isoformat() + 'Z', (date + pd.DateOffset(days=2)).isoformat() + 'Z')
            result = self.influx.query(query)
            df = result['DWD_REA6']

        else:

            if date.year != self.last_sim_year:
                self.hist_year = np.random.randint(low=1995, high=2018)
                self.last_sim_year = date.year

            if '02-29' in str(date) or '02-29' in str(date + pd.DateOffset(days=1)):
                date -= pd.DateOffset(days=1)

            query = 'select * from "DWD_REA6" where time >= \'%s\' and time < \'%s\'' \
                    % (date.replace(self.hist_year).isoformat() + 'Z',
                      (date.replace(self.hist_year) + pd.DateOffset(days=2)).isoformat() + 'Z')
            result = self.influx.query(query)
            df = result['DWD_REA6']


        self.influx.switch_database(self.database)
        keys = {'dir': 'DNI',       # direct radiation  [W/m²] new key = DNI
                'dif': 'DHI',       # direct radiation  [W/m²] new key = DHI
                'ws': 'Ws',         # windspeed         [m/s]  new key = Ws
                'temp': 'TAmb'}     # temperature       [°C]   new key = TAmb

        for hash in self.map_hash.index:
            df_area = df.loc[df['geohash'] == hash, :]
            df_area = df_area.rename(columns=keys)
            df_area['TAmb'] -= 273.15
            df_area['Ws'] = np.min((df_area['Ws'].to_numpy(), np.ones_like(df_area['Ws'].to_numpy()) * 20), axis=0)
            df_area['GHI'] = df_area['DNI'] + df_area['DHI']
            df_area.tz_convert('UTC')
            self.influx.write_points(dataframe=df_area, measurement='weather',
                                     tag_columns=['area', 'lon', 'lat', 'geohash'])

if __name__ == "__main__":

    my_generator = WeatherGenerator(database='MAS2020_40')
    my_generator.generate_weather(True, pd.to_datetime('2018-01-01'))