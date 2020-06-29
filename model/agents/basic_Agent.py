import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
import configparser
import logging
from interfaces.interface_Influx import influxInterface
from interfaces.interface_mongo import mongoInterface
from apps.frcst_DEM import typFrcst as demTyp
from apps.frcst_Price import annFrcst as priceTyp
from apps.frcst_Weather import weatherForecast
import pandas as pd
import pika
import numpy as np


class agent:

    def __init__(self, date, plz, typ='PWP', exchange='Market'):

        config = configparser.ConfigParser()
        config.read(r'./app.cfg')

        database = config['Results']['Database']
        mongoHost = config['MongoDB']['Host']
        influxHost = config['InfluxDB']['Host']
        marketHost = config['Market']['Host']

        # Metadaten eines Agenten
        self.name = typ + '_%i' % plz  # Name
        self.plz = plz  # Gebiet
        self.date = pd.to_datetime(date)  # aktueller Tag
        self.typ = typ  # Agententyp (RES,PWP,DEM,...)
        self.delay = 2

        self.errorCounter = 0
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.INFO)
        fh = logging.FileHandler(r'./logs/%s.log' % self.name)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        # Log-File für jeden Agenten (default-Level Warning, Speicherung unter ./logs)
        # logging.basicConfig(filename=r'./logs/%s_fix.log' % self.name, level=logging.INFO, filemode='a')
        # logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
        # Verbindingen an die Datenbanken sowie den Marktplatz
        self.ConnectionInflux = influxInterface(host=influxHost, database=database)            # Datenbank zur Speicherung der Zeitreihen
        self.ConnectionMongo = mongoInterface(host=mongoHost, database=database, area=plz)     # Datenbank zur Speicherung der Strukurdaten

        # Laden der Geoinfomationen
        if len(self.ConnectionMongo.getPosition()) == 0:
            print('Nummer: %s ist kein offizielles PLZ-Gebiet' % plz)
            print(' --> Aufbau des Agenten %s_%s beendet' % (typ, plz))
            exit()
        else:
            self.geo = self.ConnectionMongo.getPosition()['geohash']


        # Anbindung an MQTT
        credentials = pika.PlainCredentials('dMAS', 'dMAS2020')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=marketHost,heartbeat=0, credentials=credentials))
        self.receive = self.connection.channel()
        self.receive.exchange_declare(exchange=exchange, exchange_type='fanout')
        self.result = self.receive.queue_declare(queue=self.name, exclusive=True)
        self.queue_name = self.result.method.queue
        self.receive.queue_bind(exchange=exchange, queue=self.queue_name)

        # Prognosemethoden eines Agenten
        self.forecasts = {
            'demand': demTyp(self.ConnectionInflux),
            'weather': weatherForecast(self.ConnectionInflux),
            'price': priceTyp(self.ConnectionInflux, init=np.random.randint(8, 22))
        }

    def weatherForecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        """ Wetterprognose des jeweiligen PLZ-Gebietes"""
        weather = dict(wind=[], dir=[], dif=[], temp=[])
        for i in range(days):
            w = self.forecasts['weather'].forecast(str(self.geo), date)
            for key, value in w.items():
                weather[key] += value
        return weather

    def priceForecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        """ Preisprognose für MCP, Braun- und Steinkohle, Kernkraft und Gas"""
        price = dict(power=[], gas=[], co=[], lignite=3.5, coal=8.5, nuc=1)
        for i in range(days):
            demand = self.forecasts['demand'].forecast(date)
            p = self.forecasts['price'].forecast(date, demand)
            for key, value in p.items():
                if key in ['power', 'gas', 'co']:
                    price[key] += value

        return price

    def demandForecast(self, date=pd.to_datetime('2019-01-01'), days=1):
        """ Lastprognose für Gesamtdeutschland"""
        demand = []
        for i in range(days):
            demand += list(self.forecasts['demand'].forecast(date))
        return np.asarray(demand).reshape((-1,))

    def post_actual(self):
        print('post actual')

    def callback(self, ch, method, properties, body):
        """ Methodenaufruf zugehörig zum Marktsignal"""
        message = body.decode("utf-8")
        self.date = pd.to_datetime(message.split(' ')[1])
        # Aufruf Regelleistungsmarkt
        if 'opt_balancing' in message:
            try:
                self.optimize_balancing()
            except Exception as inst:
                self.exceptionHandle(part='Balancing Plan', inst=inst)
        # Aufruf DayAhead-Markt
        if 'opt_dayAhead' in message:
            try:
                self.optimize_dayAhead()
            except Exception as inst:
                self.exceptionHandle(part='Day Ahead Plan', inst=inst)
        # Aufruf Ergebnisse DayAhead-Markt
        if 'result_dayAhead' in message:
            try:
                self.post_dayAhead()
            except Exception as inst:
                self.exceptionHandle(part='Day Ahead Result', inst=inst)
        # Aufruf Berechnung der aktuellen Erzeugung und Verbrauch
        if 'opt_actual' in message:
            try:
                self.optimize_actual()
            except Exception as inst:
                self.exceptionHandle(part='Actual Plan', inst=inst)
        # Aufruf Bereitstellung Regelenergie und Planung des nächsten Tages
        if 'result_actual' in message:
            try:
                self.post_actual()
            except Exception as inst:
                self.exceptionHandle(part='Actual Results', inst=inst)
        # Aufruf zum Beenden
        if 'kill' in message:
            self.ConnectionInflux.influx.close()
            self.ConnectionMongo.logout(self.name)
            self.ConnectionMongo.mongo.close()
            if self.receive.is_open:
                self.receive.close()
                self.connection.close()
            print('terminate area')
            exit()

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
            for key, func in self.intelligence.items():
                func.counter += 1
                if func.counter >= func.collect:
                    func.fit()
                    func.counter = 0

    def run_agent(self):
        """ Verbinden des Agenten mit der Marktplattform und Warten auf Anweisungen """
        self.receive.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)
        print(' --> Agent %s hat sich mit dem Marktplatz verbunden, wartet auf Anweisungen (To exit press CTRL+C)'
              % self.name)
        self.receive.start_consuming()

    def exceptionHandle(self, part, inst):
        print(self.name)
        print('Error in ' + part)
        print('Error --> ' + str(inst))
        # logging.error('%s --> %s' % (part, inst))
        self.errorCounter += 1
        if self.errorCounter == 5:
            self.ConnectionMongo.logout(self.name)
            self.errorCounter = 0

if __name__ == "__main__":
    agent = agent(date='2019-01-01', plz=1)
    # agent.ConnectionRest.login(agent.name, agent.typ)
    # agent.run_agent()
