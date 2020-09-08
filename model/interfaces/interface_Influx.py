import numpy as np
import pandas as pd
from influxdb import InfluxDBClient
import multiprocessing
from joblib import Parallel, delayed

class influxInterface:

    def __init__(self, host='149.201.88.150', port=8086, user='root', password='root', database='MAS_XXXX',
                 year=2019):
        self.influx = InfluxDBClient(host, port, user, password, database)
        self.influx.switch_database(database)

        self.histWeatherYear = np.random.randint(low=2005, high=2015)
        self.switchWeatherYear = year

        self.maphash = pd.read_excel(r'./data/InfoGeo.xlsx', index_col=0)
        self.maphash = self.maphash.set_index('hash')

        self.windSmoothFactors = pd.read_csv(r'./data/smoothWind.csv', index_col=0)

        self.database = database

    def saveData(self, json_body):
        self.influx.switch_database(database=self.database)
        self.influx.write_points(json_body)

    """----------------------------------------------------------
    -->             Query Wetterdaten                        <---    
    ----------------------------------------------------------"""

    def __writeWeatherdata(self, date, valid):

        if valid:

            self.influx.switch_database('weatherData')
            start = date.isoformat() + 'Z'
            end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
            query = 'select * from "DWD_REA6" where time >= \'%s\' and time < \'%s\'' % (start, end)
            result = self.influx.query(query)
            self.influx.switch_database(self.database)
            json_body = []

            for point in result.get_points():

                json_body.append(
                    {
                        "measurement": "weather",
                        "tags": {
                            "geohash": point['geohash'],
                            "area": point['area'],
                            "lat": point['lat'],
                            "lon": point['lon']
                        },
                        "time": point['time'],
                        "fields": {
                            "GHI": np.float(point['dir'] + point['dif']),       # Globalstrahlung           [W/m²]
                            "DNI": np.float(point['dir']),                      # Direkte Strahlung         [W/m²]
                            "DHI": np.float(point['dif']),                      # Diffuse Stahlung          [W/m²]
                            "TAmb": np.float(point['temp']-273.15),             # Temperatur                [°C]
                            "Ws": min(20.0, np.float(point['ws']))              # Windgeschindigkeit        [m/s]
                        }
                    }
                )

            self.influx.write_points(json_body)

        else:

            if date.year != self.switchWeatherYear:
                self.histWeatherYear = np.random.randint(low=1995, high=2017)
                self.switchWeatherYear = date.year
            self.influx.switch_database('weatherData')

            if '02-29' in str(date):
                date -= pd.DateOffset(days = 1)

            start = date.replace(self.histWeatherYear).isoformat() + 'Z'
            end = (date.replace(self.histWeatherYear) + pd.DateOffset(days=1)).isoformat() + 'Z'
            query = 'select * from "DWD_REA6" where time >= \'%s\' and time < \'%s\'' % (start, end)
            result = self.influx.query(query)
            self.influx.switch_database(self.database)
            json_body = []

            for point in result.get_points():
                json_body.append(
                    {
                        "measurement": "weather",
                        "tags": {
                            "geohash": point['geohash'],
                            "area": point['area'],
                            "lat": point['lat'],
                            "lon": point['lon']
                        },
                        "time": str(date.year) + point['time'][4:],
                        "fields": {
                            "GHI": np.float(point['dir'] + point['dif']),       # Globalstrahlung           [W/m²]
                            "DNI": np.float(point['dir']),                      # Direkte Strahlung         [W/m²]
                            "DHI": np.float(point['dif']),                      # Diffuse Stahlung          [W/m²]
                            "TAmb": np.float(point['temp']-273.15),             # Temperatur                [°C]
                            "Ws": min(20.0, np.float(point['ws']))              # Windgeschindigkeit        [m/s]
                        }
                    }
                )

            self.influx.write_points(json_body)

        print('Wetter für %s geschrieben' % date.date())


    def generateWeather(self, start, end, valid=True):
        """ Wettergenerator für die Simualtionsdauer (start --> end) """

        dateRange = pd.date_range(start=start, end=end, freq='D')
        num_cores = min(multiprocessing.cpu_count(), 50, len(dateRange))
        print('Starte Wetterziehung')
        Parallel(n_jobs=num_cores)(delayed(self.__writeWeatherdata)(i, valid) for i in dateRange)

    def getWeather(self, geo, date, smoothWind=False):
        """ Wetter im jeweiligen PLZ-Gebiet am Tag X """
        self.influx.switch_database(database=self.database)
        geohash = str(geo)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        if smoothWind == True:
            area = self.maphash.loc[self.maphash.index == geohash, 'PLZ'].to_numpy()[0]
            N = self.windSmoothFactors.loc[self.windSmoothFactors.index == area, 'smooth factor'].to_numpy()[0]
            if N > 0:
                lb = int(N/2)
                ub = int(N / 2) + int(N % 2 > 0)
            else:
                lb = 0
                ub = 0
            startWind = (date - pd.DateOffset(hours=lb)).isoformat() + 'Z'
            endWind = (date + pd.DateOffset(days=1, hours=ub)).isoformat() + 'Z'
        else:
            N = 0
            startWind = start
            endWind = end

        keys = {'DNI': 'dir',           # Direkte Strahlung         [W/m²]          DNI
                'DHI': 'dif',           # Diffuse Stahlung          [W/m²]          DHI
                'Ws': 'wind',           # Windgeschwindigkeit       [m/s] (2m)      Ws
                'TAmb': 'temp'}         # Temperatur                [°C]            TAmb

        dict_ = {}

        for key, value in keys.items():
            # --> Abfrage der Daten
            if value == 'wind':
                query = 'select mean("%s") from "weather" where time >= \'%s\' and time < \'%s\' and geohash = \'%s\' GROUP BY time(1h) fill(0)'\
                        % (key, startWind, endWind, geohash)
            else:
                query = 'select mean("%s") from "weather" where time >= \'%s\' and time < \'%s\' and geohash = \'%s\' GROUP BY time(1h) fill(0)'\
                        % (key, start, end, geohash)

            result = self.influx.query(query)

            if result.__len__() > 0:
                if value == 'wind' and N > 0:
                    values = [np.round(point['mean'], 2) for point in result.get_points()]
                    smoothedWind = []
                    for j in range(24):
                        ws = 0
                        for i in range(j, j + ub + lb + 1):
                            ws += values[i]
                        smoothedWind.append(ws/(N+1))
                    dict_.update({value: smoothedWind})
                else:
                    dict_.update({value: [np.round(point['mean'], 2) for point in result.get_points()]})

            else:
                dict_.update({value: list(np.zeros(24))})

        return dict_

    def getWind(self, date):
        """ mittlere Windgeschwindigkeit in [m/s] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage der Daten
        query = 'select mean("Ws") from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            wind = np.asarray([np.round(point['mean']*np.random.normal(loc=1, scale=0.05), 2) for point in result.get_points()])
        else:
            wind = 4.0*np.ones(24)
        return np.asarray(wind).reshape((-1, 1))

    def getIrradiation(self, date):
        """ mittlere Bestrahlungssträke in [W/m²]"""
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage der Daten
        query = 'select mean("GHI") from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            rad = np.asarray([np.round(point['mean']*np.random.normal(loc=1, scale=0.05), 2) for point in result.get_points()])
        else:
            rad = np.zeros(24)
        return np.asarray(rad).reshape((-1, 1))

    def getTemperature(self, date):
        """ mittlere Temperatur in [W/m²]"""
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select mean("TAmb") from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            temp = np.asarray([np.round(point['mean']*np.random.normal(loc=1, scale=0.05), 2) for point in result.get_points()])
        else:
            temp = 13 * np.ones(24)
        return np.asarray(temp).reshape((-1, 1))

    """----------------------------------------------------------
    -->             Query DayAhead Vermarktung               <---    
    ----------------------------------------------------------"""

    def getDayAheadAsk(self, date, name, days=1):
        """ Verkaufsvolumen in [MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=days)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and' \
                ' "order"=\'ask\' GROUP BY time(1h) fill(previous)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            ask = np.asarray([np.round(point['sum'], 2) if point['sum'] is not None else 0 for point in result.get_points()])  # -- volume [MWh]
        else:
            ask = np.zeros(days*24)
        return np.asarray([a if ask is not None else 0 for a in ask])


    def getDayAheadBid(self, date, name, days=1):
        """ Kaufsvolumen in [MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=days)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' ' \
                'and "order"=\'bid\' GROUP BY time(1h) fill(0)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            bid = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])  # -- volume [MWh]
        else:
            bid = np.zeros(days*24)
        return np.asarray([b if bid is not None else 0 for b in bid])

    def getDayAheadPrice(self, date, days=1):
        """ Marktclearing Preis in [€/MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=days)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("price") from "DayAhead" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            mcp = np.asarray([point['sum'] for point in result.get_points()])
        else:
            mcp = np.zeros(days*24)
        return np.asarray([m if m is not None else 0 for m in mcp]).reshape((-1, 1))


    def getPowerArea(self, date, area):

        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        ask = 'SELECT sum("power") FROM "DayAhead" WHERE ("order" = \'%s\' AND "area" = \'%s\') AND time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
              % ('ask', area, start, end)

        result = self.influx.query(ask)

        if result.__len__() > 0:
            ask = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])
        else:
            ask = np.zeros(24)

        bid = 'SELECT sum("power") FROM "DayAhead" WHERE ("order" = \'%s\' AND "area" = \'%s\') AND time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
              % ('bid', area, start, end)

        result = self.influx.query(bid)

        if result.__len__() > 0:
            bid = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])
        else:
            bid = np.zeros(24)

        return bid - ask


    """----------------------------------------------------------
    -->             Query Regelleistung                      <---    
    ----------------------------------------------------------"""

    def getBalancingPower(self, date, name):
        """ Bezuschlagte positive und negative Regelleistung in [MW] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage der Daten
        # positive Regelleistung
        query = 'select sum("power") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' ' \
                'and "order"=\'pos\' GROUP BY time(4h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            pos = np.asarray([np.round(point['sum'], 2) for point in result.get_points() for _ in range(4)])
        else:
            pos = np.zeros(24)
        # negative Regelleistung
        query = 'select sum("power") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' ' \
                'and "order"=\'neg\' GROUP BY time(4h) fill(0)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            neg = np.asarray([np.round(point['sum'], 2) for point in result.get_points() for _ in range(4)])
        else:
            neg = np.zeros(24)

        return pos, neg

    def getBalancingEnergy(self, date, name):
        """ Bezuschlagte positive und negative Regelenergie in [MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage der Daten
        # postive Regelenergie
        query = 'select sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and ' \
                '"order"=\'pos\' GROUP BY time(1h) fill(0)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            pos = np.asarray([point['sum'] for point in result.get_points()])
        else:
            pos = np.zeros(24)
        # negative Regelenergie
        query = 'select sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and ' \
                '"order"=\'neg\' GROUP BY time(1h) fill(0)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            neg = np.asarray([point['sum'] for point in result.get_points()])
        else:
            neg = np.zeros(24)

        return pos, neg

    def getBalancingPowerFees(self, date):
        """ Gebühren resultierend aus der Bereitstellung der Leistung [€]"""
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage der Daten
        query = 'select sum("power")*sum("powerPrice") from "Balancing" where "order" =\'pos\' and ' \
                'time >= \'%s\' and time < \'%s\' GROUP BY time(4h) fill(0)' % (start, end)
        resultPos = self.influx.query(query)
        posVal = [np.round(point['sum_sum'], 2) for point in resultPos.get_points()]
        query = 'select sum("power")*sum("powerPrice") from "Balancing" where "order" =\'neg\' and ' \
                'time >= \'%s\' and time < \'%s\' GROUP BY time(4h) fill(0)' % (start, end)
        resultNeg = self.influx.query(query)
        negVal = [np.round(point['sum_sum'], 2) for point in resultNeg.get_points()]

        return posVal + negVal

    def getBalEnergy(self, date, names):
        self.influx.switch_database(database=self.database)
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        df = pd.DataFrame()

        for name in names:
            for typ in ['pos', 'neg']:
                query = 'select sum("energyPrice"), sum("power") from "Balancing" where "order" =\'%s\' and ' \
                        'time >= \'%s\' and time < \'%s\' and "agent"=\'%s\' GROUP BY time(4h) fill(0)' % (typ, start, end, name)
                result = self.influx.query(query)

                val = [[np.round(point['sum'] if point['sum'] is not None else float(0), 2),
                        np.round(point['sum_1'] if point['sum'] is not None else float(0), 2),
                        typ, name] for point in result.get_points()]
                val = np.asarray(val)
                df = df.append(pd.DataFrame(val, columns=['price','quantity','typ','name']))

        df['quantity'] = df['quantity'].to_numpy(dtype=np.float)

        return df

    """----------------------------------------------------------
    -->             Query Leistungsplanung                   <---    
    ----------------------------------------------------------"""

    def getPowerScheduling(self, date, name, timestamp, days=1):
        """ Leistung zum jeweilgen Planungsschritt in [MW] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=days)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end, name, timestamp)
        if 'DEM' in name:
            query = 'select sum("powerTotal") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'%s\' GROUP BY time(1h) fill(0)' \
                    % (start, end, name, timestamp)
        result = self.influx.query(query)
        if result.__len__() > 0:
           return np.asarray([point['sum'] for point in result.get_points()])
        else:
            return np.zeros(days*24)

    def getTotalDemand(self, date):
        """ Gesamtlast in Deutschland in [MW] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'SELECT sum("Power") FROM "Areas" WHERE time >= \'%s\' and time < \'%s\'  and "typ" =\'DEM\' and "timestamp" = \'optimize_dayAhead\' GROUP BY time(1h) fill(0)' % (
        start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            demand = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])
        else:
            demand = 35000*np.ones(24)
        return np.asarray(demand).reshape((-1,1))

if __name__ == "__main__":
    pass