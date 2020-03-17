# Importe
import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
print(os.getcwd())
from interfaces.interface_rest import restInferace
from interfaces.interface_Influx import influxInterface
from interfaces.interface_mongo import mongoInterface
from apps.qLearn_BalPower import learnBalancingPower
from apps.qLearn_DayAhead import learnDayAheadMarginal
from apps.frcst_DEM import demandForecast
from apps.frcst_Price import priceForecast
from apps.frcst_Weather import weatherForecast
import pandas as pd
import geohash2
import pika
import pymongo

class agent:

    def __init__(self, date, plz, typ='RES', host='149.201.88.150', exchange='DayAhead'):

        self.name = typ + '_%i' %plz                                             # -- Agent name
        self.area = plz
        self.date = pd.to_datetime(date)                                         # -- current day
        self.typ = typ                                                           # -- Typ (RES,PWP,DEM,...)
        self.error_counter = 0                                                   # -- Error Counter

        self.restCon = restInferace()
        self.influxCon = influxInterface()
        self. mongoCon = mongoInterface()

        credentials = pika.PlainCredentials('MAS_2019', 'FHAACHEN!')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,heartbeat=0,credentials=credentials))
        self.receive = self.connection.channel()
        self.receive.exchange_declare(exchange=exchange, exchange_type='fanout')
        self.result = self.receive.queue_declare(queue='', exclusive=True)
        self.queue_name = self.result.method.queue
        self.receive.queue_bind(exchange=exchange, queue=self.queue_name)

        self.mongo = pymongo.MongoClient('mongodb://' + host + ':27017/')
        self.mongodb = self.mongo["MAS_2019"]
        self.mongoTable = self.mongodb["system_data"]

        self.forecasts = {
            'demand': demandForecast(self.influxCon.influx),
            'weather': weatherForecast(self.influxCon.influx),
            'price': priceForecast(self.influxCon.influx)
        }

        self.intelligence = {
            'Balancing': learnBalancingPower(initT=10),
            'DayAhead': learnDayAheadMarginal(initT=10)
        }

        df = pd.read_csv('./data/PlzGeo.csv', index_col=0)
        geo = df.loc[df['PLZ'] == plz, ['Latitude', 'Longitude']]
        self.geo = geohash2.encode(float(geo.Latitude), float(geo.Longitude))

    # ----- Forecast Methods -----
    def weatherForecast(self):
        return self.forecasts['weather'].forecast(str(self.geo),self.date)

    def priceForecast(self):
        demand = self.forecasts['demand'].forecast(self.date)
        return self.forecasts['price'].forecast(self.date, demand)

    def demandForecast(self):
        return self.forecasts['demand'].forecast(self.date)

    # ----- Learning Next Day -----
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

    # ----- Routines -----
    def optimize_balancing(self):
        print('optimize balancing')
        self.restCon.sendBalancing(dict(uuid='RES_1'))

    def optimize_dayAhead(self):
        print('optimize day ahead')
        self.restCon.sendDayAhead(dict(uuid='RES_1'))

    def post_dayAhead(self):
        print('post day ahead')

    def optimize_actual(self):
        print('optimize actual')
        self.restCon.sendBalancing(dict(uuid='RES_1'))

    def post_actual(self):
        print('post actual')

    def callback(self, ch, method, properties, body):

        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])

        if 'opt_balancing' in message:
            try:
                self.optimize_balancing()
            except Exception as inst:
                self.exceptionHandle(part='Balancing Plan', inst=inst)
        if 'opt_dayAhead' in message:
            try:
                self.optimize_dayAhead()
            except Exception as inst:
                self.exceptionHandle(part='Day Ahead Plan', inst=inst)
        if 'result_dayAhead' in message:
            try:
                self.post_dayAhead()
            except Exception as inst:
                self.exceptionHandle(part='Day Ahead Result', inst=inst)
        if 'opt_actual' in message:
            try:
                self.optimize_actual()
            except Exception as inst:
                self.exceptionHandle(part='Actual Plan', inst=inst)
        if 'result_actual' in message:
            try:
                self.post_actual()
            except Exception as inst:
                self.exceptionHandle(part='Actual Results', inst=inst)

        if 'kill' in message:
            self.restCon.logout(self.name)
            self.receive.close()
            self.restCon.influx.close()
            self.mongoCon.mongo.close()
            print('terminate area')

    def run_agent(self):
        self.receive.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.receive.start_consuming()

    # ----- Exception Handling -----
    def exceptionHandle(self, part, inst):
        print('Error in ' + part)
        print('Error --> ' + str(inst))
        self.error_counter += 1
        print('Error Counter ' + str(self.error_counter))
        if self.error_counter > 5:
            print('logout')
            self.restInterface.logout()
            self.error_counter = 0
            self.restInterface.login()

if __name__ == "__main__":

    agent = agent(date='2019-01-01', plz=1)
    agent.restCon.login(agent.name, agent.typ)
    agent.run_agent()