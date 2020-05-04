import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.res_Port import resPort
from agents.basic_Agent import agent as basicAgent
from apps.qLearn_DayAhead import qLeran as daLearning
import logging
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=52, help='PLZ-Agent')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_XXXX', help='Name der Datenbank')
    return parser.parse_args()

class tsoAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='TSO', dbName=dbName)

        logging.info('Start des Agenten')

        self.portfolio = resPort(typ='RES', gurobi=False)
        # Einbindung der Solardaten aus der MongoDB (EEG Anlagen vor 2013)
        solarsystems = self.ConnectionMongo.getPvEEG(plz)
        total = 0
        for key, system in solarsystems.items():
            total += system['maxPower']
        self.portfolio.Cap_Solar = np.round(total / 1000, 2)
        name = 'EEG2013_%s' % plz
        generator = dict(maxPower=np.round(total, 2), eta=0.15, area=7, typ='solar')
        self.portfolio.addToPortfolio(name, {name: generator})
        logging.info('PV-Erzeugung hinzugefügt')

        logging.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        """Einsatzplanung für den Regelleistungsmarkt"""
        logging.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()                                                  # Oderbook für alle Gebote (Stunde 1-24)
        # Prognosen für den kommenden Tag
        weather = self.weatherForecast()                                    # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast()                                        # Preisdaten (power,gas,nuc,coal,lignite)
        prc = np.asarray(price['power']).reshape((-1, 1))  # MCP Porgnose
        demand = self.demandForecast()                                      # Lastprognose
        self.portfolio.setPara(self.date, weather, price, demand)
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)             # Berechnung der Einspeiseleitung

        # Füge für jede Stunde die entsprechenden Gebote hinzu
        for i in range(self.portfolio.T):
            # biete immer den minimalen Preis, aber nie mehr als den maximalen Preis
            quantity = [-1*power[i]]
            price = [float(-499.89)]
            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='optimize_dayAhead', typ='RES', EEG='True'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i], Wind=self.portfolio.pWind[i], Solar=self.portfolio.pSolar[i],
                                   PriceFrcst=prc[i][0])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('Planung DayAhead-Markt abgeschlossen')

    def post_dayAhead(self):
        """Reaktion auf  die DayAhead-Ergebnisse"""

        # Speichern der Daten und Aktionen, um aus diesen zu lernen
        self.qLearn.collectData(self.date, self.actions.reshape((24, 1)))

        # Abfrage der DayAhead Ergebnisse
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)
        price = self.ConnectionInflux.getDayAheadPrice(self.date)
        profit = [(ask[i]-bid[i])*price[i] for i in range(24)]
        power = ask - bid
        # print('%s (%s, %s): %s' % (self.name, self.portfolio.Cap_Wind, self.portfolio.Cap_Solar, profit))
        planing = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_dayAhead')
        difference = np.asarray(planing).reshape((-1,)) - np.asarray(power).reshape((-1,))

       # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='post_dayAhead', typ='RES', EEG='True'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i], Difference=difference[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'post_dayAhead')

        # Berechnung der Prognoseabweichung
        self.portfolio.buildModel(response=schedule)
        actual = self.portfolio.fixPlaning()                    # Berechung des aktuellen Fahrplans + Errors
        # Berechnung der Abweichung
        difference = np.asarray([(schedule[i] - actual[i] if schedule[i] > 0 else 0.00) for i in self.portfolio.t])
        power = np.asarray([(actual[i] if schedule[i] > 0 else 0.00) for i in self.portfolio.t])
        # Aufbau der "Gebote" (Abweichungen zum gemeldeten Fahrplan)
        orderbook = dict()
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': difference[i], 'hour': i, 'name': self.name}})
        self.ConnectionMongo.setActuals(name=self.name, date=self.date, orders=orderbook)

        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='optimize_actual', typ='RES', EEG='True'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i], Difference=difference[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        power = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')     # Letzter bekannter Fahrplan

        # Abspeichern der Ergebnisse
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='post_actual', typ='RES', EEG='True'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)


        logging.info('Tag %s abgeschlossen' %self.date)
        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = tsoAgent(date='2019-01-01', plz=args.plz, mongo=args.mongo, influx=args.influx,
                     market=args.market, dbName=args.dbName)
    # agent.ConnectionMongo.login(agent.name, False)
    # try:
    #     agent.run_agent()
    # except Exception as e:
    #     logging.error('Fehler in run_agent: %s' %e)
    # finally:
    #     agent.ConnectionInflux.influx.close()
    #     agent.ConnectionMongo.logout(agent.name)
    #     agent.ConnectionMongo.mongo.close()
    #     if agent.receive.is_open:
    #         agent.receive.close()
    #         agent.connection.close()
    #     exit()
