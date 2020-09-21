# third party modules
import numpy as np
import pandas as pd
from influxdb import DataFrameClient
import multiprocessing
from joblib import Parallel, delayed


class InfluxInterface:

    def __init__(self, host='149.201.88.150', port=8086, user='root', password='root', database='MAS2020_12',
                 year=2018):
        # self.influx = InfluxDBClient(host, port, user, password, database)
        self.influx = DataFrameClient(host, port, user, password, database)
        self.influx.switch_database(database)

        self.histWeatherYear = np.random.randint(low=2005, high=2015)
        self.switchWeatherYear = year

        self.maphash = pd.read_csv(r'./data/Ref_GeoInfo.csv', index_col=0, sep=';', decimal=',')
        self.maphash = self.maphash.set_index('hash')

        self.database = database

    def save_data(self, df, measurement, tags={}):
        df.tz_localize('UTC')
        self.influx.switch_database(database=self.database)
        self.influx.write_points(df, measurement, tags, protocol='line')

    def __write_weather(self, date, valid):

        keys = {'dir': 'DNI',       # direct radiation  [W/m²] new key = DNI
                'dif': 'DHI',       # direct radiation  [W/m²] new key = DHI
                'ws':   'Ws',       # windspeed         [m/s]  new key = Ws
                'temp': 'TAmb'}     # temperature       [°C]   new key = TAmb

        if valid:

            self.influx.switch_database('weatherData')
            query = 'select * from "DWD_REA6" where time >= \'%s\' and time < \'%s\'' \
                    % (date.isoformat() + 'Z', (date + pd.DateOffset(days=1)).isoformat() + 'Z')
            result = self.influx.query(query)
            df = result['DWD_REA6']
            self.influx.switch_database(self.database)

            for h in self.maphash.index:
                df_area = df.loc[df['geohash'] == h, :]
                df_area = df_area.rename(columns=keys)
                df_area['TAmb'] -= 273.15
                df_area['Ws'] = np.min((df_area['Ws'].to_numpy(), np.ones_like(df_area['Ws'].to_numpy())*20), axis=0)
                df_area['GHI'] = df_area['DNI'] + df_area['DHI']
                df_area.tz_convert('UTC')
                self.influx.write_points(dataframe=df_area, measurement='weather',
                                         tag_columns=['area', 'lon', 'lat', 'geohash'])
        else:

            # select historical weather year and switch if a new year begins
            if date.year != self.switchWeatherYear:
                self.histWeatherYear = np.random.randint(low=1995, high=2018)
                self.switchWeatherYear = date.year
            # ignore leap year
            if '02-29' in str(date):
                date -= pd.DateOffset(days=1)

            self.influx.switch_database('weatherData')
            query = 'select * from "DWD_REA6" where time >= \'%s\' and time < \'%s\'' \
                    % (date.replace(self.histWeatherYear).isoformat() + 'Z',
                       (date.replace(self.histWeatherYear) + pd.DateOffset(days=1)).isoformat() + 'Z')
            result = self.influx.query(query)

            df = result['DWD_REA6']
            self.influx.switch_database(self.database)

            for h in self.maphash.index:
                df_area = df.loc[df['geohash'] == h, :]
                df_area = df_area.rename(columns=keys)
                df_area['TAmb'] -= 273.15
                df_area['Ws'] = np.min((df_area['ws'].to_numpy(), np.ones_like(df_area['ws'].to_numpy())*20), axis=0)
                self.influx.write_points(dataframe=df_area, measurement='weather',
                                         tag_columns=['area', 'lon', 'lat', 'geohash'])

    def generate_weather(self, start, end, valid=True):
        date_range = pd.date_range(start=start, end=end, freq='D')
        num_cores = min(multiprocessing.cpu_count(), 50, len(date_range))
        print('generate weather information for simulation period')
        Parallel(n_jobs=num_cores)(delayed(self.__write_weather)(i, valid) for i in date_range)
        print('weather generation complete')

    def get_weather(self, geo, date, mean=False):

        self.influx.switch_database(database=self.database)         # change to simulation database
        dict_ = {}                                                  # return result in dict
        # mapping database to simulation keys
        keys = {'DNI': 'dir',                                       # direct radiation  [W/m²] new key = dir
                'DHI': 'dif',                                       # direct radiation  [W/m²] new key = dif
                'Ws': 'wind',                                       # windspeed         [m/s]  new key = wind
                'TAmb': 'temp'}                                     # temperature       [°C]   new key = temp

        # used for qLearning and price forecast
        if mean:
            start = date.isoformat() + 'Z'                          # start day
            end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'  # end day
            # get dir, dif, wind and temp
            for key, value in keys.items():
                # select key as value in  time for germany in each hour
                query = 'select mean("%s") as "%s" from "weather" where time >= \'%s\' ' \
                        'and time < \'%s\' GROUP BY time(1h) fill(0)' % (key, value, start, end)
                result = self.influx.query(query)

                if result.__len__() > 0:
                    dict_.update({value: result['weather'][value].to_numpy()})
                else:
                    dict_.update({value: np.zeros(24)})
        # used for optimization and generation calculation
        else:
            lb = 0                                                  # default 0
            ub = 0                                                  # default 0
            # read area size for smoothing the wind speed
            n = self.maphash.loc[self.maphash.index == geo, 'Windfactor'].to_numpy()[0]
            if n > 0:
                lb = int(n/2)
                ub = int(n / 2) + int(n % 2 > 0)
            # get dir, dif, wind and temp
            for key, value in keys.items():
                if value != 'wind':
                    start = date.isoformat() + 'Z'
                    end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
                else:
                    start = (date - pd.DateOffset(hours=lb)).isoformat() + 'Z'
                    end = (date + pd.DateOffset(days=1, hours=ub)).isoformat() + 'Z'
                # select key as value  in area and time for each hour
                query = 'select mean("%s") as "%s" from "weather" where time >= \'%s\' and time < \'%s\' ' \
                        'and geohash = \'%s\' GROUP BY time(1h) fill(0)' % (key, value, start, end, geo)
                result = self.influx.query(query)
                if result.__len__() > 0:
                    values = result['weather'][value].to_numpy()
                    # smooth wind according to area size n
                    if value == 'wind' and n > 0:
                        smoothed_wind = []
                        for j in range(24):
                            ws = 0
                            for i in range(j, j + ub + lb + 1):
                                ws += values[i]
                            smoothed_wind.append(ws/(n+1))
                        dict_.update({value: np.asarray(smoothed_wind).reshape((-1,))})
                    else:
                        dict_.update({value: values})
                else:
                    dict_.update({value: np.zeros(24)})

        return dict_

    def get_ask_da(self, date, name, days=1):

        self.influx.switch_database(database=self.database)     # change to simulation database
        # select ask for agent in time period
        query = 'select sum("power") as "ask" from "DayAhead" ' \
                'where time >= \'%s\' and time < \'%s\' and "names" = \'%s\' and' \
                ' "order"=\'ask\' GROUP BY time(1h) fill(previous)' \
                % (date.isoformat() + 'Z', (date + pd.DateOffset(days=days)).isoformat() + 'Z', name)

        result = self.influx.query(query)

        if result.__len__() > 0:
            ask = result['DayAhead']["ask"].to_numpy()          # volume [MWh]
        else:
            ask = np.zeros(days*24)

        return np.nan_to_num(ask).reshape((-1,))

    def get_bid_da(self, date, name, days=1):

        self.influx.switch_database(database=self.database)     # change to simulation database
        # select bid for agent in time period
        query = 'select sum("power") as "bid" from "DayAhead" ' \
                'where time >= \'%s\' and time < \'%s\' and "names" = \'%s\' ' \
                'and "order"=\'bid\' GROUP BY time(1h) fill(0)' \
                % (date.isoformat() + 'Z', (date + pd.DateOffset(days=days)).isoformat() + 'Z', name)

        result = self.influx.query(query)

        if result.__len__() > 0:
            bid = result['DayAhead']["bid"].to_numpy()      # volume [MWh]
        else:
            bid = np.zeros(days*24)

        return np.nan_to_num(bid).reshape((-1,))

    def get_prc_da(self, date, days=1):

        self.influx.switch_database(database=self.database)     # change to simulation database
        # select market clearing price in time period
        query = 'select sum("price") as "price" from "DayAhead" ' \
                'where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
                % (date.isoformat() + 'Z', (date + pd.DateOffset(days=days)).isoformat() + 'Z')

        result = self.influx.query(query)

        if result.__len__() > 0:
            mcp = result['DayAhead']["price"].to_numpy()        # price [€/MWh]
        else:
            mcp = np.zeros(days*24)

        return np.nan_to_num(mcp).reshape((-1,))

    def get_dem(self, date):

        self.influx.switch_database(database=self.database)     # change to simulation database

        query = 'SELECT sum("Power") as "power" FROM "Areas" ' \
                'WHERE time >= \'%s\' and time < \'%s\'  and "typ" =\'DEM\' and "timestamp" = \'optimize_dayAhead\' ' \
                'GROUP BY time(1h) fill(0)' \
                % (date.isoformat() + 'Z', (date + pd.DateOffset(days=1)).isoformat() + 'Z')
        result = self.influx.query(query)
        if result.__len__() > 0:
            demand = result['Areas']["power"].to_numpy()        # power demand [MW]
        else:
            demand = 35000*np.ones(24)

        return np.asarray(demand).reshape((-1,))


if __name__ == "__main__":
    myInterface = InfluxInterface(database='MAS2020_10')
    pass
