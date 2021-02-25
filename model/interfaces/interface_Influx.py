# third party modules
import numpy as np
import pandas as pd
from influxdb import DataFrameClient


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

        plz_codes = self.maphash['PLZ'].to_numpy(dtype=str)
        self.plz_codes = []
        for code in plz_codes:
            if len(code) == 1:
                self.plz_codes.append('0' + code)
            else:
                self.plz_codes.append(code)

        self.database = database

    def save_data(self, df, measurement, tags={}):
        df.tz_localize('UTC')
        self.influx.switch_database(database=self.database)
        self.influx.write_points(df, measurement, tags, protocol='line')

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
        """Get powerflow and s_nom of one line from InfluxDB for specified date"""
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

    def get_lines_data(self, date):
        """Get powerflow and s_nom of all lines from InfluxDB for specified date"""
        ts = date.isoformat() + 'Z'

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


    def get_typ_generation(self,start, end, typ):
        start = start.isoformat() + 'Z'
        end = end.isoformat() + 'Z'

        query = 'SELECT sum("powerTotal") FROM "Areas" WHERE ("timestamp" = \'post_dayAhead\' AND "typ" = \'%s\')' \
                'AND time >= \'%s\' AND time < \'%s\' GROUP BY "area" fill(null)' % (typ, start, end)

        results = self.influx.query(query)
        powers = {code: 0 for code in self.plz_codes}
        for key, element in results.items():
            code = key[1][0][1]
            if len(code) == 1:
                code = '0' + code
            power = int(element['sum'].values[0] / 10 ** 3)
            powers.update({code: power})
        df = pd.DataFrame.from_dict(powers, orient='index', columns=['power'])
        df['plz'] = df.index

        return df


    def get_range_generation(self,start, end, typ):

        days = (end - start).days

        aggregate = '1h'
        if days > 3:
           aggregate = '1d'
        if days > 14:
            aggregate= '7d'
        if days > 60:
            aggregate = '30d'

        start = start.isoformat() + 'Z'
        end = end.isoformat() + 'Z'

        map = {'wind':      'powerWind',
               'solar':     'powerSolar',
               'coal':      'powerCoal',
               'lignite':   'powerLignite',
               'nuc':       'powerNuc',
               'gas':       'powerGas'}

        query = 'SELECT sum("%s") FROM "Areas" WHERE ("timestamp" = \'post_dayAhead\')' \
                'AND time >= \'%s\' AND time < \'%s\' GROUP BY time(%s), "area" fill(0)' % (map[typ], start, end,
                                                                                            aggregate)

        results = self.influx.query(query)
        codes, powers, dates = [], [], []
        for key, value in results.items():
            code = key[1][0][1]
            if len(code) == 1:
                code = '0' + code
            for i in range(len(value)):
                powers.append(value.iloc[i, 0])
                dates.append(str(value.index[i]).split('+')[0])
                codes.append(code)

        df = pd.DataFrame(data=dict(power=powers, date=dates, plz=codes))

        return df

if __name__ == "__main__":
    import json
    import plotly.express as px
    import plotly.io as pio
    pio.renderers.default = "browser"

    with open(r'./data/germany.geojson') as file:
        areas = json.load(file)

    for feature in areas['features']:
        feature.update({'id': feature['properties']['plz']})


    myInterface = InfluxInterface(database='MAS2020_40', host='149.201.88.83')
    #bid = myInterface.get_bid_da(date=pd.to_datetime('2018-01-05'), name='PWP_49')  # volume to sell
    #ask = myInterface.get_ask_da(date=pd.to_datetime('2018-01-05'), name='PWP_49')  # volume to sell

    df = myInterface.get_range_generation(start=pd.to_datetime('2018-01-01'),
                                          end=pd.to_datetime('2018-01-14'), typ='solar')


    fig = px.choropleth_mapbox(df, geojson=areas, color='power', locations='plz', animation_frame="date",
                               # color_continuous_scale='greys',
                               range_color=(df['power'].min(), df['power'].max()),
                               mapbox_style="carto-positron",
                               zoom=4.5, center = {"lat": 51.3, "lon": 10.2},
                               opacity=0.5,
                               labels={'power': 'E [GWh]'})

    fig.show()

    # prc = InfluxInterface.get_prc_da(self.date)  # market clearing price