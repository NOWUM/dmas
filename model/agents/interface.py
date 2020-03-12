from influxdb import InfluxDBClient
import pika
import requests
import pandas as pd
import pymongo
import numpy as np
import geohash2
from apps.qLearn_BalPower import learnBalancingPower
from apps.qLearn_DayAhead import learnDayAheadMarginal
from apps.frcst_DEM import demandForecast
from apps.frcst_Price import priceForecast
from apps.frcst_Weather import weatherForecast

class Interface:

    # ----- Agent Methods -----
    # ------------------------------------------------------------------------------------------------------
    def __init__(self, date, plz, host='149.201.88.150', exchange='DayAhead', typ='Agent',
                 influx_login=dict(user='root', password='root', port=8086, dbname='MAS_2019')):

        self.name = typ + '_%i' %plz                                             # -- Agent name
        self.area = plz
        self.date = pd.to_datetime(date)                                         # -- current day
        self.typ = typ                                                           # -- Typ (RES,PWP,DEM,...)
        self.error_counter = 0                                                   # -- Error Counter

        # -- Geo PLZ Hash
        df = pd.read_csv('./data/PlzGeo.csv', index_col=0)
        geo = df.loc[df['PLZ'] == plz, ['Latitude', 'Longitude']]
        self.geo = geohash2.encode(float(geo.Latitude), float(geo.Longitude))
        # -- Connection to MongoDB
        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.mongodb = self.mongo["MAS_2019"]
        self.mongoTable = self.mongodb["system_data"]
        # -- Connection to InfluxDB
        self.influx = InfluxDBClient(host, influx_login['port'], influx_login['user'], influx_login['password'],influx_login['dbname'])
        self.influx.switch_database(influx_login['dbname'])
        # -- Connection to RabbitMQ
        self.market_plattform = 'http://' + host + ':5010/'
        credentials = pika.PlainCredentials('MAS_2019', 'FHAACHEN!')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,heartbeat=0,credentials=credentials))
        self.receive = self.connection.channel()
        self.receive.exchange_declare(exchange=exchange, exchange_type='fanout')
        self.result = self.receive.queue_declare(queue='', exclusive=True)
        self.queue_name = self.result.method.queue
        self.receive.queue_bind(exchange=exchange, queue=self.queue_name)

        self.forecasts = {
            'demand': demandForecast(self.influx),
            'weather': weatherForecast(self.influx),
            'price': priceForecast(self.influx)
        }

        self.intelligence = {
            'Balancing': learnBalancingPower(initT=10),
            'DayAhead': learnDayAheadMarginal(initT=10)
        }

    # ----- login & logout function -----
    # ------------------------------------------------------------------------------------------------------
    def login(self):
        # -- DEM
        if self.typ == 'DEM':
            r = requests.post(self.market_plattform + 'login', json={'uuid': self.name, 'area' : self.name.split('_')[1],
                                                                     'typ': self.name.split('_')[0], 'reserve' : 'O'})
        # -- RES & PWP
        else:
            r = requests.post(self.market_plattform + 'login', json={'uuid': self.name, 'area' : self.name.split('_')[1],
                                                                     'typ': self.name.split('_')[0], 'reserve' : 'X'})
        # -- login complete
        if r.ok: print('login complete: ' + str(r.json()))

    def logout(self):
        r = requests.post(self.market_plattform + 'logout', json={'uuid': self.name})
        # -- logout complete
        if r.ok: print('logout complete: ' + str(r.json()))

    # ----- wait for market commands -----
    # ------------------------------------------------------------------------------------------------------
    def run_agent(self):
        self.receive.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.receive.start_consuming()

    # ----- forecast methods -----
    # ------------------------------------------------------------------------------------------------------
    def weatherForecast(self):          # -- Weather forecast (Query InfluxDB)
        return self.forecasts['weather'].forecast(str(self.geo),self.date)

    def priceForecast(self):           # -- price forecast
        demand = self.forecasts['demand'].forecast(self.date)
        return self.forecasts['price'].forecast(self.date, demand)

    def demandForecast(self):
        return self.forecasts['demand'].forecast(self.date)

    def nextDay(self):

        for key, method in self.forecasts.items():
            if key != 'weather':
                method.collectData(self.date)
                method.counter += 0

                if method.counter >= method.collect:
                    method.fitFunction()
                    method.counter = 0

        if self.typ != 'DEM':
            for _, func in self.intelligence.items():
                func.counter += 1
                if func.counter >= func.collect:
                    func.fit()
                    func.counter = 0

    # ----- GET Results from InfluxDB -----
    # ------------------------------------------------------------------------------------------------------
    def dayAheadResults(self):
        # -- Build-up query for InfluxDB
        start = self.date.isoformat() + 'Z'
        end = (self.date + pd.DateOffset(days=1)).isoformat() + 'Z'

        # -- Get Ask-Results
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'ask\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            ask = np.asarray([np.round(point['sum'],2) for point in result.get_points()])       # -- volume [MWh]
        else:
            ask = np.zeros(24)
        # -- Get Bid-Results
        query = 'select sum("power") from "DayAhead" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'bid\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            bid = np.asarray([np.round(point['sum'], 2) for point in result.get_points()])      # -- volume [MWh]
        else:
            bid = np.zeros(24)
        # -- Get MCP
        query = 'select sum("price") from "DayAhead" where time >= \'%s\' and time < \'%s\' GROUP BY time(1h) fill(0)' %(start, end)
        result = self.influx.query(query)
        if result.__len__() > 0:
            price = np.asarray([point['sum'] for point in result.get_points()])               # -- price [€/MWh]
        else:
            price = 3000*np.ones(24)

        reward = sum(ask*price) - sum(bid*price)

        return ask, bid, reward

    def balancingResults(self):
        # -- Build-up query for InfluxDB
        start = self.date.isoformat() + 'Z'
        end = (self.date + pd.DateOffset(days=1)).isoformat() + 'Z'

        # -- Get Result postive Balancing
        query = 'select sum("power"), sum("powerPrice") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'pos\' GROUP BY time(4h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            pos = np.asarray([np.round(point['sum'], 2) for point in result.get_points()for _ in range(4)])
            price = np.asarray([np.round(point['sum_1'], 2) for point in result.get_points()for _ in range(4)])
        else:
            pos = np.zeros(24)
            price = np.zeros(24)
        reward = sum(pos*price)/6
        # -- Get Result negative Balancing
        query = 'select sum("power"), sum("powerPrice") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'neg\' GROUP BY time(4h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            neg = np.asarray([np.round(point['sum'], 2) for point in result.get_points() for _ in range(4)])
            price = np.asarray([np.round(point['sum_1'], 2) for point in result.get_points() for _ in range(4)])
        else:
            neg = np.zeros(24)
            price = np.zeros(24)

        return pos, neg, (reward+sum(neg*price)/6)

    def powerDAPlan(self):
        # -- Build-up query for InfluxDB
        start = self.date.isoformat() + 'Z'
        end = (self.date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("Power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'optimize_dayAhead\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            power = np.asarray([point['sum'] for point in result.get_points()])
        else:
            power = np.zeros(24)

        return power

    def powerDAResult(self):
        # -- Build-up query for InfluxDB
        start = self.date.isoformat() + 'Z'
        end = (self.date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("Power") from "Areas" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "timestamp" = \'post_dayAhead\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            power = np.asarray([point['sum'] for point in result.get_points()])
        else:
            power = np.zeros(24)

        return power

    def powerBalResult(self):
        # -- Build-up query for InfluxDB
        start = self.date.isoformat() + 'Z'
        end = (self.date + pd.DateOffset(days=1)).isoformat() + 'Z'

        query = 'select sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'pos\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            pos = np.asarray([point['sum'] for point in result.get_points()])
        else:
            pos = np.zeros(24)

        query = 'select sum("energyPrice")*sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'pos\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            rewardPos = np.asarray([point['sum_sum'] for point in result.get_points()])
        else:
            rewardPos = np.zeros(24)

        query = 'select sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'neg\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            neg = np.asarray([point['sum'] for point in result.get_points()])
        else:
            neg = np.zeros(24)

        query = 'select sum("energyPrice")*sum("energy") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' and "order"=\'neg\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            rewardNeg = np.asarray([point['sum_sum'] for point in result.get_points()])
        else:
            rewardNeg = np.zeros(24)

        query = 'select sum("cost") from "Balancing" where time >= \'%s\' and time < \'%s\' and "agent" = \'%s\' GROUP BY time(1h) fill(0)' \
                % (start, end, self.name)
        result = self.influx.query(query)
        if result.__len__() > 0:
            cost = np.asarray([point['sum'] for point in result.get_points()])
        else:
            cost = np.zeros(24)

        return pos, neg, np.sum(rewardPos + rewardNeg) - np.sum(cost)


# ----- Exception Handling -----
# ------------------------------------------------------------------------------------------------------
    def exceptionHandle(self, part, inst):
        print('Error in ' + part)
        print('Error --> ' + str(inst))
        self.error_counter += 1
        print('Error Counter ' + str(self.error_counter))
        if self.error_counter > 5:
            print('logout')
            self.logout()
            self.error_counter = 0
            self.login()

if __name__ == "__main__":

    agent = Interface(date=pd.to_datetime('2019-01-07'), plz=19, typ='DEM')
    # prices = agent.priceForecast()