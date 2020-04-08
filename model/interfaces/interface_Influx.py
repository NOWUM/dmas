import numpy as np
import pandas as pd
from influxdb import InfluxDBClient

class influxInterface:

    def __init__(self, host='149.201.88.150', port=8086, user='root', password='root', database='MAS_XXXX',
                 year=2019):
        self.influx = InfluxDBClient(host, port, user, password, database)
        self.influx.switch_database(database)

        self.histWeatherYear = np.random.randint(low=2005, high=2015)
        self.switchWeatherYear = year

        self.maphash = pd.read_excel(r'./data/InfoGeo.xlsx', index_col=0)
        self.maphash = self.maphash.set_index('hash')

        self.database = database

    def saveData(self, json_body):
        self.influx.switch_database(database=self.database)
        self.influx.write_points(json_body)

    """----------------------------------------------------------
    -->             Query Wetterdaten                        <---    
    ----------------------------------------------------------"""

    def generateWeather(self, start, end):
        """ Wettergenerator für die Simualtionsdauer (start --> end) """
        for date in pd.date_range(start=start, end=end, freq='D'):
            if date.year != self.switchWeatherYear:
                self.histWeatherYear = np.random.randint(low=2005, high=2015)
                self.switchWeatherYear = date.year
            # Zugriff auf die Datenbank mit den historischen Wetterdaten
            self.influx.switch_database('weather')
            try: # Fehler bei Schaltjahr abfangen
                start = date.replace(self.histWeatherYear).isoformat() + 'Z'
            except:
                date = date - pd.DateOffset(days=1)
                start = date.replace(self.histWeatherYear).isoformat() + 'Z'
            end = (date.replace(self.histWeatherYear) + pd.DateOffset(days=1)).isoformat() + 'Z'

            if '0229' in start:
                start = start.replace('2902', '2802')
            # --> Abfrage der Daten
            query = 'select * from "germany" where time > \'%s\' and time < \'%s\'' % (start, end)
            result = self.influx.query(query)
            # Wechsel zur Simulationsdatenbank
            self.influx.switch_database(self.database)
            json_body = []
            for data in result['germany']:
                json_body.append(
                    {
                        "measurement": "weather",
                        "tags": {
                            "geohash": data['geohash'],
                            "plz": self.maphash.loc[self.maphash.index==data['geohash'], 'PLZ'].to_numpy()[0]
                        },
                        "time": str(date.year) + data['time'][4:],
                        "fields": {
                            "GHI": np.float(data['GHI']),               # Globalstrahlung           [W/m²]
                            "DNI": np.float(data['DNI']),               # Direkte Strahlung         [W/m²]
                            "DHI": np.float(data['DHI']),               # Diffuse Stahlung          [W/m²]
                            "TAmb": np.float(data['TAmb']),             # Temperatur                [°C]
                            "Ws": np.float(data['Ws'])                  # Windgeschwindigkeit       [m/s] (2m)
                        }
                    }
                )
            self.influx.write_points(json_body)
            print('Wetter für %s geschrieben' % date.date())

    def getWeather(self, geo, date):
        """ Wetter im jeweiligen PLZ-Gebiet am Tag X """
        self.influx.switch_database(database=self.database)
        geohash = str(geo)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage der Daten
        query = 'select * from "weather" where time >= \'%s\' and time <= \'%s\' and geohash = \'%s\'' \
                % (start, end, geohash)
        result = self.influx.query(query)
        if result.__len__() > 0:
            dir = [point['DNI'] for point in result.get_points()]           # Direkte Strahlung         [W/m²]
            dif = [point['DHI'] for point in result.get_points()]           # Diffuse Stahlung          [W/m²]
            wind = [point['Ws'] for point in result.get_points()]           # Windgeschwindigkeit       [m/s] (2m)
            TAmb = [point['TAmb'] for point in result.get_points()]         # Temperatur                [°C]
        else:
            dir = list(np.zeros(24))
            dif = list(np.zeros(24))
            wind = list(np.zeros(24))
            TAmb = list(np.zeros(24))

        return dict(wind=wind, dir=dir, dif=dif, temp=TAmb)

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
            wind = np.asarray([np.round(point['mean'], 2) for point in result.get_points()])
        else:
            wind =  4.0*np.ones(24)
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
            rad = np.asarray([np.round(point['mean'], 2) for point in result.get_points()])
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
            temp = np.asarray([np.round(point['mean'], 2) for point in result.get_points()])
        else:
            temp = 13 * np.ones(24)
        return np.asarray(temp).reshape((-1, 1))

    """----------------------------------------------------------
    -->             Query DayAhead Vermarktung               <---    
    ----------------------------------------------------------"""

    def getDayAheadAsk(self, date, name):
        """ Verkaufsvolumen in [MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and' \
                ' "order"=\'ask\' GROUP BY time(1h) fill(0)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            return np.asarray([np.round(point['sum'], 2) for point in result.get_points()])  # -- volume [MWh]
        else:
            return np.zeros(24)
        return ask

    def getDayAheadBid(self, date, name):
        """ Kaufsvolumen in [MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' ' \
                'and "order"=\'bid\' GROUP BY time(1h) fill(0)' % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            bid = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])  # -- volume [MWh]
        else:
            bid = np.zeros(24)
        return bid

    def getDayAheadPrice(self, date):
        """ Marktclearing Preis in [€/MWh] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("price") from "DayAhead" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            mcp = np.asarray([point['sum'] for point in result.get_points()])
        else:
            mcp =  np.zeros(24)
        return np.asarray(mcp).reshape((-1, 1))

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

    def getPowerScheduling(self, date, name, timestamp):
        """ Leistung zum jeweilgen Planungsschritt in [MW] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'select sum("Power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end, name, timestamp)
        result = self.influx.query(query)
        if result.__len__() > 0:
           return np.asarray([point['sum'] for point in result.get_points()])
        else:
            return np.zeros(24)

    def getTotalDemand(self, date):
        """ Gesamtlast in Deutschland in [MW] """
        self.influx.switch_database(database=self.database)
        # Tag im ISO Format
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'
        # --> Abfrage
        query = 'SELECT sum("Power") FROM "Areas" WHERE time >= \'%s\' and time < \'%s\'  and "timestamp" = \'optimize_dayAhead\' GROUP BY time(1h) fill(0)' % (
        start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            demand =  np.asarray([np.round(point['sum'], 2) for point in result.get_points()])
        else:
            demand = 35000*np.ones(24)
        return np.asarray(demand).reshape((-1,1))

if __name__ == "__main__":
    pass