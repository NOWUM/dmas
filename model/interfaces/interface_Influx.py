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
                'where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(null)' \
                % (date.isoformat() + 'Z', (date + pd.DateOffset(days=days)).isoformat() + 'Z')

        result = self.influx.query(query)

        if result.__len__() > 0:
            mcp = result['DayAhead']["price"].to_numpy()        # price [€/MWh]
        else:
            mcp = [37.70, 35.30, 33.90, 33.01, 33.27, 35.78, 43.17, 50.21, 52.89, 51.18, 48.24, 46.72, 44.23,
                   42.29, 41.60, 43.12, 45.37, 50.95, 55.12, 56.34, 52.70, 48.20, 45.69, 40.25]
            mcp = np.asarray(mcp)

        return np.nan_to_num(mcp).reshape((-1,))

    def get_dem(self, date):

        self.influx.switch_database(database=self.database)     # change to simulation database
        #print("get_dem->database: ", self.database)#debug print
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

    def get_line_data(self, date, line):
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query_ask = 'SELECT sum("p0") as "power_flow" FROM "Grid" ' \
                    'WHERE time >= \'%s\' and time < \'%s\' and "name" = \'%s\'' \
                    'GROUP BY time(1h) fill(0)' \
                    % (start, end, line)

        result_ask = self.influx.query(query_ask)

        if result_ask.__len__() > 0:
            power_flow = result_ask['Grid']['power_flow'].to_numpy()
        else:
            power_flow = np.zeros(24)

        query_ask = 'SELECT sum("s_nom") as "s_nom" FROM "Grid" ' \
                    'WHERE time >= \'%s\' and time < \'%s\' and "name" = \'%s\'' \
                    'GROUP BY time(1h) fill(0)' \
                    % (start, end, line)

        result_ask = self.influx.query(query_ask)

        if result_ask.__len__() > 0:
            s_nom = result_ask['Grid']['s_nom'].to_numpy()
        else:
            s_nom = np.zeros(24)

        return power_flow, s_nom

    #SELECT sum("p0") FROM "Grid" WHERE $timeFilter GROUP BY time(1h), "name" fill(null)
    def get_lines_data(self, date):
        ts = date.isoformat() + 'Z'

        # query_ask = 'SELECT sum("p0") as "power_flow", sum("s_nom") as "s_nom" FROM "Grid" ' \
        #             'WHERE time >= \'%s\' and time < \'%s\'' \
        #             'GROUP BY time(1h), "name" fill(0)' \
        #             % (start, end)

        query_ask = 'SELECT sum("p0") as "power_flow", sum("s_nom") as "s_nom" FROM "Grid" ' \
                    'WHERE time = \'%s\'' \
                    'GROUP BY time(1h), "name" fill(0)' \
                    % (ts)

        result = self.influx.query(query_ask)

        line_data = {}

        for key, value in result.items():
            if len(value['power_flow']) > 0:
                line_data.update({key[1][0][1]: {'power': value['power_flow'].to_numpy(),
                                             's_nom': value['s_nom'].to_numpy()}})
            else:
                line_data.update({key[1][0][1]: {'power': np.zeros(1),
                                                 's_nom': np.inf*np.ones(1)}})

        return line_data

    def get_power_area(self, date, area):
        """Get power from InfluxDB for specified date and PLZ"""
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        # ask:
        query_ask = 'SELECT sum("power") as "power_ask" FROM "DayAhead" ' \
                'WHERE time >= \'%s\' and time < \'%s\' and "order" = \'%s\' and "area" = \'%s\' ' \
                'GROUP BY time(1h) fill(0)' \
                % (start, end, 'ask', area)

        result_ask = self.influx.query(query_ask)

        if result_ask.__len__() > 0:
            ask = result_ask['DayAhead']['power_ask'].to_numpy()  # power demand [MW]
        else:
            ask = np.zeros(24)

        # bid:
        query_bid = 'SELECT sum("power") as "power_bid" FROM "DayAhead" ' \
                'WHERE time >= \'%s\' and time < \'%s\' and "order" = \'%s\' and "area" = \'%s\' ' \
                'GROUP BY time(1h) fill(0)' \
                % (start, end, 'bid', area)

        result_bid = self.influx.query(query_bid)

        if result_bid.__len__() > 0:
            bid = result_bid['DayAhead']['power_bid'].to_numpy()  # power generation [MW]
        else:
            bid = np.zeros(24)

        return bid - ask # bid>ask=Verbraucher   ask=Anbieter(Fragen Preis nach) bid=bieten Preis


if __name__ == "__main__":
    #myInterface = InfluxInterface()
    #myInterface = InfluxInterface(database='MAS2020_10')
    myInterface = InfluxInterface(database='MAS2020_30', host='149.201.196.100')
    #x = myInterface.get_lines_data(pd.to_datetime('2018-01-01'))
    #prices = myInterface.get_prc_da(date=pd.to_datetime('2018-01-01') - pd.DateOffset(days=7))
    from collections import deque
    from apps.misc_Dummies import createSaisonDummy
    # from apps.frcst_Price import annFrcst
    #
    # test = annFrcst()
    # for date in pd.date_range(start='2018-01-01', end='2018-02-01'):
    #     dem = myInterface.get_dem(date)  # demand germany [MW]
    #     weather = myInterface.get_weather('u302eujrq6vg', date, mean=True)  # mean weather germany
    #     prc = myInterface.get_prc_da(date)
    #     prc_1 = myInterface.get_prc_da(date - pd.DateOffset(days=1))  # mcp yesterday [€/MWh]
    #     prc_7 = myInterface.get_prc_da(date - pd.DateOffset(days=7))  # mcp week before [€/MWh]
    #     test.collect_data(date, dem, weather, prc, prc_1, prc_7)
    # test.fit_function()
    # date = pd.to_datetime('2018-02-02')
    # dem = myInterface.get_dem(date)  # demand germany [MW]
    # weather = myInterface.get_weather('u302eujrq6vg', date, mean=True)  # mean weather germany
    # # prc = myInterface.get_prc_da(date)
    # prc_1 = myInterface.get_prc_da(date - pd.DateOffset(days=1))  # mcp yesterday [€/MWh]
    # prc_7 = myInterface.get_prc_da(date - pd.DateOffset(days=7))  # mcp week before [€/MWh]
    # prices = test.forecast(date, dem, weather, prc_1, prc_7)



        #dummies = createSaisonDummy(date, date).reshape((-1,))
        #x = np.hstack((dem, weather['wind'], weather['dir'] + weather['dif'], weather['temp'], prc_1, prc_7, dummies))
        #y = myInterface.get_prc_da(date)



    # dx = deque(maxlen=10)
    # dy = deque(maxlen=10)




    # collect input data
    #self.demMatrix = np.concatenate((self.demMatrix, dem.reshape((-1, 24))))
    #self.wndMatrix = np.concatenate((self.wndMatrix, weather['wind'].reshape((-1, 24))))
    #self.radMatrix = np.concatenate((self.radMatrix, (weather['dir'] + weather['dif']).reshape((-1, 24))))
    #self.tmpMatrix = np.concatenate((self.tmpMatrix, weather['temp'].reshape((-1, 24))))
    #self.prc_1Matrix = np.concatenate((self.prc_1Matrix, prc_1.reshape((-1, 24))))
    #self.prc_7Matrix = np.concatenate((self.prc_7Matrix, prc_7.reshape((-1, 24))))
    #self.dummieMatrix = np.concatenate((self.dummieMatrix, createSaisonDummy(date, date).reshape((-1, 24))))
    # collect output data
    # self.mcpMatrix = np.concatenate((self.mcpMatrix, prc.reshape((-1, 24))))
    # x = np.concatenate((self.demMatrix, self.radMatrix, self.wndMatrix, self.tmpMatrix,  # Step 0: load (build) data
    #                     self.prc_1Matrix, self.prc_7Matrix, self.dummieMatrix), axis=1)
    # x = np.concatenate((self.x, x), axis=0)  # input data
    # y = np.concatenate((self.y, self.mcpMatrix), axis=0)  # output data
    # self.scaler.partial_fit(x)  # Step 1: scale data
    # x_std = self.scaler.transform(x)  # Step 2: split data
    # X_train, X_test, y_train, y_test = train_test_split(x_std, y, test_size=0.2)
    # self.model.fit(X_train, y_train)  # Step 3: fit model
    # self.fitted = True  # Step 4: set fitted-flag to true


    # for date in pd.date_range(start='2018-01-01', end='2018-02-01'):
    #     dem = myInterface.get_dem(date)  # demand germany [MW]
    #     weather = myInterface.get_weather('u302eujrq6vg', date, mean=True)  # mean weather germany
    #     prc_1 = myInterface.get_prc_da(date - pd.DateOffset(days=1))  # mcp yesterday [€/MWh]
    #     prc_7 = myInterface.get_prc_da(date - pd.DateOffset(days=7))  # mcp week before [€/MWh]
    #     dummies = createSaisonDummy(date, date).reshape((-1,))
    #     x = np.hstack((dem, weather['wind'], weather['dir'] + weather['dif'], weather['temp'], prc_1, prc_7, dummies))
    #     y = myInterface.get_prc_da(date)
    #
    #     dx.append(x)
    #     dy.append(y)


        #d.append(np.hstack((prc_1,prc_7)))
