# third party modules
from sys import exit #fixes NameError: name 'exit' is not defined
import os
import pandas as pd
import numpy as np
import time as tme
import pika
import configparser
import logging

# model modules
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from interfaces.interface_Influx import InfluxInterface
from interfaces.interface_mongo import mongoInterface
from apps.frcst_Dem import typFrcst as demandForecast
from apps.frcst_Price import annFrcst as priceForecast
from apps.frcst_Weather import weatherForecast
from apps.qLearn_DayAhead import qLeran as daLearning


class agent:

    def __init__(self, date, plz, typ='PWP'):

        config = configparser.ConfigParser()                        # read config to initialize connection
        config.read(r'./app.cfg')

        self.exchange = config['Market']['Exchange']
        self.agentSuffix = config['Market']['AgentSuffix']

        # declare meta data for each agent
        self.name = typ + '_%i' % plz + self.agentSuffix            # name
        self.plz = plz                                              # area
        self.date = pd.to_datetime(date)                            # current day
        self.typ = typ                                              # generation or consumer typ

        # dictionary for performance measuring
        self.performance = dict(initModel=0,                        # build model for da optimization
                                optModel=0,                         # optimize for da market
                                saveSchedule=0,                     # save optimization results in influx db
                                buildOrders=0,                      # construct order book
                                sendOrders=0,                       # send orders to mongodb
                                adjustResult=0,                     # adjustments corresponding to da results
                                saveResult=0,                       # save adjustments in influx db
                                nextDay=0)                          # preparation for coming day

        database = config['Results']['Database']                    # name of simulation database
        mongo_host = config['MongoDB']['Host']                      # server where mongodb runs
        influx_host = config['InfluxDB']['Host']                    # server where influxdb runs
        market_host = config['Market']['Host']                      # server where mqtt runs

        print('BasicAgent->Database: ', database)#TODO:remove debug print?
        # connections to simulation infrastructure
        self.connections = {
            'mongoDB' : mongoInterface(host=mongo_host, database=database, area=plz),   # connection mongodb
            'influxDB': InfluxInterface(host=influx_host, database=database)            # connection influxdb
        }

        # check if area is valid
        if self.connections['mongoDB'].getPosition() is None:
            print('Number: %s is no valid area' % plz)
            print(' --> stopping %s_%s' % (typ, plz))
            exit()
        else:
            self.geo = self.connections['mongoDB'].getPosition()['geohash']

        if config.getboolean('Market', 'Local'):
            con = pika.BlockingConnection(pika.ConnectionParameters(host=market_host, heartbeat=0))
            self.connections.update({'connectionMQTT': con})
        else:
            crd = pika.PlainCredentials('dMAS', 'dMAS2020')
            con = pika.BlockingConnection(pika.ConnectionParameters(host=market_host, heartbeat=0, credentials=crd))
            self.connections.update({'connectionMQTT': con})

        receive = con.channel()
        receive.exchange_declare(exchange=self.exchange, exchange_type='fanout')
        result = receive.queue_declare(queue=self.name, exclusive=True)
        receive.queue_bind(exchange=self.exchange, queue=result.method.queue)
        self.connections.update({'exchangeMQTT': receive})

        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(r'./logs/%s.log' % self.name)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.disabled = True

        # forecast methods
        self.forecasts = {
            'demand': demandForecast(),
            'weather': weatherForecast(self.connections['influxDB']),
            'price': priceForecast(init=np.random.randint(8, 22))
        }
        # Parameters for the trading strategy on the day ahead market
        self.strategy = dict(
            maxPrice=np.zeros(24) ,                                   # maximal price of each hour
            minPrice=np.zeros(24),                                    # minimal price of each hour
            actions=np.zeros(24),                                     # different actions (slopes)
            epsilon=0.7,                                              # factor to find new actions
            lr=0.8,                                                   # learning rate
            qLearn=daLearning(init=np.random.randint(5, 10 + 1)),     # interval for learning
            delay=2                                                   # start offset
        )

    def weather_forecast(self, date=pd.to_datetime('2019-01-01'), days=1, mean=False):
        weather = dict(wind=[], dir=[], dif=[], temp=[])
        for i in range(days):
            # get weather data for day (i)
            w = self.forecasts['weather'].forecast(str(self.geo), date + pd.DateOffset(days=i), mean)
            for key, value in w.items():
                weather[key] = np.concatenate((weather[key], value * np.random.uniform(0.95, 1.05, 24)))
        return weather

    def price_forecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        price = dict(power=[], gas=[], co=[], lignite=3.5, coal=8.5, nuc=1)
        for i in range(days):
            # collect input parameter for day ahead price forecast at day (i)
            demand = self.forecasts['demand'].forecast(date + pd.DateOffset(days=i))
            weather = self.forecasts['weather'].forecast(str(self.geo), date + pd.DateOffset(days=i), mean=True)
            price_d1 = self.connections['influxDB'].get_prc_da(date - pd.DateOffset(days=1)).reshape((-1, 24))
            price_d7 = self.connections['influxDB'].get_prc_da(date - pd.DateOffset(days=7 - i)).reshape((-1, 24))
            # get price forecast for day (i)
            p = self.forecasts['price'].forecast(date, demand, weather, price_d1, price_d7)
            for key, value in p.items():
                if key in ['power', 'gas', 'co']:
                    price[key] = np.concatenate((price[key], value))
        return price

    def demand_forecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        demand = []
        for i in range(days):
            demand += list(self.forecasts['demand'].forecast(date))
        return np.asarray(demand).reshape((-1,))

    def perfLog(self, function, start):

        # save performance in influxDB
        timeDelta = tme.time() - start
        procssingPerfomance = [
            {
                "measurement": 'Performance',
                "tags": dict(typ=self.typ,                      # typ
                             agent=self.name,                   # name
                             area=self.plz,                     # area
                             function=function,                 # processing step
                             date=str(self.date.date())),
                "time": pd.to_datetime(tme.time(), unit='s').isoformat() + 'Z',
                "fields": dict(processingTime=timeDelta)

            }
        ]
        self.connections['influxDB'].save_data(procssingPerfomance)

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # Aufruf DayAhead-Markt
        if 'opt_dayAhead' in message:
            try:
                self.optimize_dayAhead()
            except Exception as inst:
                self.exception_handle(part='Day Ahead Plan', inst=inst)
        # Aufruf Ergebnisse DayAhead-Markt
        if 'result_dayAhead' in message:
            try:
                self.post_dayAhead()
            except Exception as inst:
                self.exception_handle(part='Day Ahead Result', inst=inst)
        # Aufruf Powerflow Berechnung
        if 'grid_calc' in message:
            try:
                if self.typ == 'NET':
                    self.calc_power_flow()
            except Exception as inst:
                self.exception_handle(part='Grid Calculation', inst=inst)
        # Aufruf zum Beenden
        if 'kill' in message:
            self.connections['influxDB'].influx.close()
            self.connections['mongoDB'].close()
            if not self.connections['connectionMQTT'].is_closed:
                self.connections['connectionMQTT'].close()
            print('terminate area')
            exit()

    def run(self):
        self.connections['exchangeMQTT'].basic_consume(queue=self.name, on_message_callback=self.callback, auto_ack=True)
        print(' --> Agent %s has connected to the marketplace, waiting for instructions (to exit press CTRL+C)'
              % self.name)
        self.connections['exchangeMQTT'].start_consuming()

    def exception_handle(self, part, inst):
        print(self.name)
        print('Error in ' + part)
        print('Error --> ' + str(inst))


if __name__ == "__main__":
    agent = agent(date='2019-01-01', plz=1)

