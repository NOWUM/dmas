import numpy as np
import pandas as pd
from influxdb import InfluxDBClient

class influxInterface:

    def __init__(self, host='149.201.88.150', port=8086 ,user='root', password='root', dbName='MAS_2019',
                 year=2019):
        self.influx = InfluxDBClient(host, port, user, password, dbName)
        self.influx.switch_database(dbName)

        self.histWeatherYear = np.random.randint(low=2005, high=2015)
        self.switchWeatherYear = year

        self.maphash = pd.read_excel(r'./data/InfoGeo.xlsx', index_col=0)
        self.maphash=self.maphash.set_index('hash')

# -- DayAhead
    def getDayAheadResult(self, date, name):
        # -- Build date
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        # -- Get Ask-Results
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'ask\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            ask = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])  # -- volume [MWh]
        else:
            ask = np.zeros(24)

        # -- Get Bid-Results
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'bid\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            bid = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])  # -- volume [MWh]
        else:
            bid = np.zeros(24)

        # -- Get MCP
        query = 'select sum("price") from "DayAhead" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' % (
        start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            price = np.asarray([point['sum'] for point in result.get_points()])  # -- price [€/MWh]
        else:
            price = 3000 * np.ones(24)

        return ask, bid, sum((ask-bid) * price)

    def getDayAheadPlan(self, date, name):
        # -- Build date
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("Power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'optimize_dayAhead\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            power = np.asarray([point['sum'] for point in result.get_points()])
        else:
            power = np.zeros(24)

        return power

    def getDayAheadSchedule(self, date, name):
        # -- Build date
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("Power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'post_dayAhead\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            power = np.asarray([point['sum'] for point in result.get_points()])
        else:
            power = np.zeros(24)

        return power

    def getActualPlan(self, date, name):
        # -- Build date
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("Power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'optimize_actual\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            power = np.asarray([point['sum'] for point in result.get_points()])
        else:
            power = np.zeros(24)

        return power

# -- Balancing
    def getBalPowerResult(self, date, name):
        # -- Build date
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        # -- Get Result postive Balancing
        query = 'select sum("power"), sum("powerPrice") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'pos\' GROUP BY time(4h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            pos = np.asarray([np.round(point['sum'], 2) for point in result.get_points() for _ in range(4)])
            price = np.asarray([np.round(point['sum_1'], 2) for point in result.get_points() for _ in range(4)])
        else:
            pos = np.zeros(24)
            price = np.zeros(24)
        rewardPos = sum(pos * price) / 6

        # -- Get Result negative Balancing
        query = 'select sum("power"), sum("powerPrice") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'neg\' GROUP BY time(4h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            neg = np.asarray([np.round(point['sum'], 2) for point in result.get_points() for _ in range(4)])
            price = np.asarray([np.round(point['sum_1'], 2) for point in result.get_points() for _ in range(4)])
        else:
            neg = np.zeros(24)
            price = np.zeros(24)
        rewardNeg = sum(neg * price) / 6

        return pos, neg, rewardPos + rewardNeg

    def getBalPowerCosts(self, date):
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("power")*sum("powerPrice") from "Balancing" where "order" =\'pos\' and ' \
                'time >= \'%s\' and time < \'%s\' GROUP BY time(4h) fill(0)' % (start, end)
        resultPos = self.influx.query(query)
        posVal = [np.round(point['sum_sum'], 2) for point in resultPos.get_points()]

        query = 'select sum("power")*sum("powerPrice") from "Balancing" where "order" =\'neg\' and ' \
                'time >= \'%s\' and time < \'%s\' GROUP BY time(4h) fill(0)' % (start, end)
        resultNeg = self.influx.query(query)
        negVal = [np.round(point['sum_sum'], 2) for point in resultNeg.get_points()]

        return posVal, negVal

    def getBalEnergyResult(self, date, name):
        date = pd.to_datetime(date)
        start = date.isoformat() + 'Z'
        end = (date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'pos\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            pos = np.asarray([point['sum'] for point in result.get_points()])
        else:
            pos = np.zeros(24)

        query = 'select sum("energyPrice")*sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'pos\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            rewardPos = np.asarray([point['sum_sum'] for point in result.get_points()])
        else:
            rewardPos = np.zeros(24)

        query = 'select sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'neg\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            neg = np.asarray([point['sum'] for point in result.get_points()])
        else:
            neg = np.zeros(24)

        query = 'select sum("energyPrice")*sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'neg\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            rewardNeg = np.asarray([point['sum_sum'] for point in result.get_points()])
        else:
            rewardNeg = np.zeros(24)

        query = 'select sum("cost") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end, name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            cost = np.asarray([point['sum'] for point in result.get_points()])
        else:
            cost = np.zeros(24)

        return pos, neg, np.sum(rewardPos + rewardNeg) - np.sum(cost)

    def getBalEnergy(self, date, names):
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

# -- Weather
    def getMeanWeather(self, start, end):

        start = start.isoformat() + 'Z'
        if start == end:
            end = (start + pd.DateOffset(days=1)).isoformat() + 'Z'
        else:
            end = end.isoformat() + 'Z'

        self.influx.switch_database('MAS_2019')
        lst = []
        for data in ['TAmb', 'GHI', 'Ws']:
            tmp = {'Date': data}  # -- Dict to save Weather
            query = 'select mean(%s) from "weather" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(null)' % (
            data, start, end)
            try:
                result = self.influx.query(query)
                result = [point for point in result.get_points()]
            except:
                result = []

            if len(result) > 0:
                tmp.update({p['time']: p['mean'] for p in result})
            else:
                tmp.update({str(i): 0 for i in pd.date_range(start=pd.to_datetime(start), end=pd.to_datetime(end), freq='60min')})
            lst.append(tmp)

        return lst

    def generateWeather(self, start, end):
        for date in pd.date_range(start=start, end=end, freq='D'):
            if date.year != self.switchWeatherYear:
                self.histWeatherYear = np.random.randint(low=2005, high=2015)
                self.switchWeatherYear = date.year

            #----- query hist. weather data ----
            self.influx.switch_database('weather')

            start = date.replace(self.histWeatherYear).isoformat() + 'Z'
            end = (date.replace(self.histWeatherYear) + pd.DateOffset(days=1)).isoformat() + 'Z'

            if '0229' in start:
                start = start.replace('2902', '2802')

            query = 'select * from "germany" where time > \'%s\' and time < \'%s\'' % (start, end)
            result = self.influx.query(query)

            #----- switch to influx simulation database -----
            self.influx.switch_database('MAS_2019')
            json_body = []
            for data in result['germany']:
                json_body.append(
                    {
                        "measurement": "weather",
                        "tags": {
                            "geohash": data['geohash'],
                            "plz": self.maphash.loc[self.maphash.index==data['geohash'],'PLZ'].to_numpy()[0]
                        },
                        "time": str(date.year) + data['time'][4:],
                        "fields": {
                            "GHI": np.float(data['GHI']),
                            "DNI": np.float(data['DNI']),
                            "DHI": np.float(data['DHI']),
                            "TAmb": np.float(data['TAmb']),
                            "Ws": np.float(data['Ws'])
                        }
                    }
                )

            self.influx.write_points(json_body)
            print('generate weather for %s' %date)

    def saveData(self, json_body):
        self.influx.write_points(json_body)

if __name__ == "__main__":
    pass