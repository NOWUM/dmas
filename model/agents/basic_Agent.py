# Importe
import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import logging
# print(os.getcwd())
from interfaces.interface_rest import restInferace
from interfaces.interface_Influx import influxInterface
from interfaces.interface_mongo import mongoInterface
from apps.qLearn_BalPower import annLearn as balLearning
from apps.qLearn_DayAhead import annLearn as daLearning
# from apps.frcst_DEM import annFrcst as demANN
from apps.frcst_DEM import typFrcst as demTyp
# from apps.frcst_Price import annFrcst as priceANN
from apps.frcst_Price import typFrcst as priceTyp
from apps.frcst_Weather import weatherForecast
import pandas as pd
import geohash2
import pika


class agent:

    def __init__(self, date, plz, typ='RES', mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', exchange='DayAhead'):

        self.name = typ + '_%i' %plz                                             # -- Agent name
        self.area = plz
        self.date = pd.to_datetime(date)                                         # -- current day
        self.typ = typ                                                           # -- Typ (RES,PWP,DEM,...)
        self.error_counter = 0                                                   # -- Error Counter
        logging.basicConfig(filename=r'./logs/%s.log' %self.name, level=logging.WARNING, format='%(levelname)s:%(message)s', filemode='w')

        self.restCon = restInferace(host=market)
        self.influxCon = influxInterface(host=influx)
        self. mongoCon = mongoInterface(host=mongo)

        credentials = pika.PlainCredentials('MAS_2019', 'FHAACHEN!')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=market,heartbeat=0, credentials=credentials))
        self.receive = self.connection.channel()
        self.receive.exchange_declare(exchange=exchange, exchange_type='fanout')
        self.result = self.receive.queue_declare(queue=self.name, exclusive=True)
        self.queue_name = self.result.method.queue
        self.receive.queue_bind(exchange=exchange, queue=self.queue_name)

        self.forecasts = {
            'demand': demTyp(self.influxCon),
            'weather': weatherForecast(self.influxCon),
            'price': priceTyp(self.influxCon)
        }

        self.intelligence = {
            'Balancing': balLearning(initT=20),
            'DayAhead': daLearning(initT=20)
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
                method.counter += 1

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
                logging.error('%s --> %s' % ('Balancing Plan', inst))
        if 'opt_dayAhead' in message:
            try:
                self.optimize_dayAhead()
            except Exception as inst:
                self.exceptionHandle(part='Day Ahead Plan', inst=inst)
                logging.error('%s --> %s' % ('Day Ahead Plan', inst))
        if 'result_dayAhead' in message:
            try:
                self.post_dayAhead()
            except Exception as inst:
                self.exceptionHandle(part='Day Ahead Result', inst=inst)
                logging.error('%s --> %s' % ('Day Ahead Result', inst))
        if 'opt_actual' in message:
            try:
                self.optimize_actual()
            except Exception as inst:
                self.exceptionHandle(part='Actual Plan', inst=inst)
                logging.error('%s --> %s' % ('Actual Plan', inst))
        if 'result_actual' in message:
            try:
                self.post_actual()
            except Exception as inst:
                self.exceptionHandle(part='Actual Results', inst=inst)
                logging.error('%s --> %s' % ('Actual Result', inst))

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
        print(self.name)
        print('Error in ' + part)
        print('Error --> ' + str(inst))
        self.error_counter += 1
        print('Error Counter ' + str(self.error_counter))
        if self.error_counter >= 5:
            print('logout')
            self.restInterface.logout()
            self.error_counter = 0
            self.restInterface.login()

if __name__ == "__main__":

    agent = agent(date='2019-01-01', plz=1)
    agent.restCon.login(agent.name, agent.typ)
    agent.run_agent()