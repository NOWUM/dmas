# third party modules
from sys import exit
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
from apps.misc_WeekPriceList import WeekPriceList
# from apps.qLearn_DayAhead import qLeran as daLearning


class agent:

    def __init__(self, date, plz, typ='PWP'):

        config = configparser.ConfigParser()                        # read config to initialize connection
        config.read(r'./agent_service.cfg')

        exchange = config['Configuration']['exchange']
        self.agentSuffix = config['Configuration']['suffix']

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

        database = config['Configuration']['database']              # name of simulation database
        mongo_host = config['Configuration']['mongodb']             # server where mongodb runs
        influx_host = config['Configuration']['influxdb']           # server where influxdb runs
        mqtt_host = config['Configuration']['rabbitmq']             # server where mqtt runs

        # connections to simulation infrastructure
        self.connections = {
            'mongoDB' : mongoInterface(host=mongo_host, database=database, area=plz),   # connection mongodb
            'influxDB': InfluxInterface(host=influx_host, database=database)            # connection influxdb
        }

        # check if area is valid
        if self.connections['mongoDB'].get_position() is None:
            print('Number: %s is no valid area' % plz)
            print(' --> stopping %s_%s' % (typ, plz))
            exit()
        else:
            self.geo = self.connections['mongoDB'].get_position()['geohash']

        if config.getboolean('Configuration', 'local'):
            con = pika.BlockingConnection(pika.ConnectionParameters(host=mqtt_host,virtual_host='SimAgent', heartbeat=0))#TODO: Heartbeat einfügen, der ausreichend hoch ist, sodass Agenten fertig rechnen können
            self.connections.update({'connectionMQTT': con})
        else:
            crd = pika.PlainCredentials('dMAS', 'dMAS2020')
            con = pika.BlockingConnection(pika.ConnectionParameters(host=mqtt_host, virtual_host='SimAgent', heartbeat=0, credentials=crd))
            self.connections.update({'connectionMQTT': con})

        receive = con.channel()
        receive.exchange_declare(exchange=exchange, exchange_type='fanout')
        result = receive.queue_declare(queue=self.name, exclusive=True)
        receive.queue_bind(exchange=exchange, queue=result.method.queue)
        self.connections.update({'exchangeMQTT': receive})

        # declare logging options
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.WARNING)
        fh = logging.FileHandler(r'./logs/%s.log' % self.name)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.disabled = True

        mcp = [37.70, 35.30, 33.90, 33.01, 33.27, 35.78, 43.17, 50.21, 52.89, 51.18, 48.24, 46.72, 44.23,
               42.29, 41.60, 43.12, 45.37, 50.95, 55.12, 56.34, 52.70, 48.20, 45.69, 40.25]
        self.week_price_list = WeekPriceList(defaultDay=mcp)

        # forecast methods
        self.forecasts = {
            'demand': demandForecast(),
            'weather': weatherForecast(self.connections['influxDB']),
            'price': priceForecast(init=30)
        }

    def weather_forecast(self, date=pd.to_datetime('2019-01-01'), days=1, mean=False):
        weather = dict(wind=[], dir=[], dif=[], temp=[])
        for i in range(days):
            # get weather data for day (i)
            w = self.forecasts['weather'].forecast(str(self.geo), date + pd.DateOffset(days=i), mean)
            for key, value in w.items():
                weather[key] = np.concatenate((weather[key], value * np.random.uniform(0.95, 1.05, 24)))
        return weather

    def price_forecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        # max forecast period is one week! --> Error
        price = dict(power=[], gas=[], co=[], lignite=3.5, coal=8.5, nuc=1)
        start = pd.to_datetime(date)
        end = pd.to_datetime(date) + pd.DateOffset(days=days-1)

        last_forecast = None
        counter = 0
        for d in pd.date_range(start=start, end=end, freq='d'):
            demand = self.forecasts['demand'].forecast(d)
            weather = self.forecasts['weather'].forecast(str(self.geo), d, mean=True)

            if last_forecast is None:
                price_d1 = self.week_price_list.get_price_yesterday()
                price_d7 = self.week_price_list.get_price_week_before()
            else:
                price_d1 = last_forecast
                price_d7 = self.week_price_list.get_price_x_days_before(max(7 - counter,1))

            #price_d1 = self.connections['influxDB'].get_prc_da(d - pd.DateOffset(days=1)).reshape((-1, 24))
            #price_d7 = self.connections['influxDB'].get_prc_da(d - pd.DateOffset(days=7 )).reshape((-1, 24))
            p = self.forecasts['price'].forecast(d, demand, weather, price_d1, price_d7)

            last_forecast = p['power']
            counter += 1
            for key, value in p.items():
                if key in ['power', 'gas', 'co']:
                    price[key] = np.round(np.concatenate((price[key], value)), 2)
                else:
                    price[key] = np.round(value, 2)
        return price

    def demand_forecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        demand = []
        for i in range(days):
            demand += list(self.forecasts['demand'].forecast(date))
        return np.asarray(demand).reshape((-1,))

    def callback(self, ch, method, properties, body):
        message = body.decode("utf-8")
        # print(message)
        self.date = pd.to_datetime(message.split(' ')[1])
        # Call DayAhead Optimization Methods for each Agent
        # -----------------------------------------------------------------------------------------------------------
        if 'opt_dayAhead' in message:
            try:
                if self.typ != 'NET' and self.typ != 'MRK':
                    self.optimize_dayAhead()
            except Exception as inst:
                self.exception_handle(part='Day Ahead Plan', inst=inst)

        # Call DayAhead Result Methods for each Agent
        # -----------------------------------------------------------------------------------------------------------
        if 'result_dayAhead' in message:
            try:
                if self.typ != 'NET' and self.typ != 'MRK':
                    self.post_dayAhead()
            except Exception as inst:
                self.exception_handle(part='Day Ahead Result', inst=inst)

        # Call for Power Flow Calculation
        # -----------------------------------------------------------------------------------------------------------
        if 'grid_calc' in message:
            try:
                if self.typ == 'NET':
                    self.calc_power_flow()
            except Exception as inst:
                self.exception_handle(part='Grid Calculation', inst=inst)

        # Call for Market Clearing
        # -----------------------------------------------------------------------------------------------------------
        if 'dayAhead_clearing' in message:
            try:
                if self.typ == 'MRK':
                    self.clearing()
            except Exception as inst:
                self.exception_handle(part='dayAhead Clearing', inst=inst)

        # Terminate Agents
        # -----------------------------------------------------------------------------------------------------------
        if 'kill' in message or self.name in message or self.typ + '_all' in message:
            self.connections['mongoDB'].logout(self.name)
            self.connections['influxDB'].influx.close()
            self.connections['mongoDB'].mongo.close()
            if not self.connections['connectionMQTT'].is_closed:
                self.connections['connectionMQTT'].close()
            print('terminate', self.name)
            exit()

    def run(self):
        self.connections['exchangeMQTT'].basic_consume(queue=self.name, on_message_callback=self.callback,
                                                       auto_ack=True)
        print(' --> Agent %s has connected to the marketplace, waiting for instructions (to exit press CTRL+C)'
              % self.name)
        self.connections['exchangeMQTT'].start_consuming()

    def exception_handle(self, part, inst):
        print(self.name)
        print('Error in ' + part)
        print('Error --> ' + str(inst))


if __name__ == "__main__":
    agent = agent(date='2019-01-01', plz=1)

