import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.dem_Port import demPort
from agents.basic_Agent import agent as basicAgent
import logging
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=6, help='PLZ-Agent')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_XXXX', help='Name der Datenbank')
    return parser.parse_args()


class demAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='DEM', dbName=dbName)

        logging.info('Start des Agenten')
        # Aufbau des Portfolios mit den enstprechenden Haushalten, Gewerbe und Industrie
        self.portfolio = demPort(typ="DEM")                             # Keine Verwendung eines Solvers

        powerH0 = 0

        pvBatteries = self.ConnectionMongo.getPVBatteries(plz)
        for key, value in pvBatteries.items():
            self.portfolio.addToPortfolio(key, {key: value})
            powerH0 += value['demandP']

        pvHeatpumps = self.ConnectionMongo.getHeatPumps(plz)
        for key, value in pvHeatpumps.items():
            self.portfolio.addToPortfolio(key, {key: value})
            powerH0 += value['demandP']

        pv = self.ConnectionMongo.getPVs(plz)
        for key, value in pv.items():
            self.portfolio.addToPortfolio(key, {key: value})
            powerH0 += value['demandP']

        demand = self.ConnectionMongo.getDemand(plz)

        name = 'plz_' + str(plz) + '_h0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['h0']*10**6 - powerH0, 2), 'typ': 'H0'}})
        logging.info('Consumer hinzugefügt')

        name = 'plz_' + str(plz) + '_g0'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['g0']*10**6, 2), 'typ': 'G0'}})
        logging.info('Gewerbe  hinzugefügt')

        name = 'plz_' + str(plz) + '_rlm'
        self.portfolio.addToPortfolio(name, {name: {'demandP': np.round(demand['rlm']*10**6, 2), 'typ': 'RLM'}})
        logging.info('Industrie hinzugefügt')

        logging.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        """Einsatzplanung für den Regelleistungsmarkt"""
        logging.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()  # Oderbook für alle Gebote (Stunde 1-24)
        # Prognosen für den kommenden Tag
        weather = self.weatherForecast()  # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast()  # Preisdaten (power,gas,nuc,coal,lignite)
        self.portfolio.setPara(self.date, weather, price)
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)  # Berechnung der Einspeiseleitung
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': [power[i]/10**3, 0], 'price': [3000, -3000], 'hour': i, 'name': self.name}})
        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)
        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz,
                                 timestamp='optimize_dayAhead', EEG='False', typ='DEM'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i]/10**3)
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('Planung DayAhead-Markt abgeschlossen')

    def post_dayAhead(self):
        """Reaktion auf  die DayAhead-Ergebnisse"""

        # Abfrage der DayAhead Ergebnisse
        #ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)
        # price = self.ConnectionInflux.getDayAheadPrice(self.date)
        # profit = [(ask[i]-bid[i])*price[i] for i in range(24)]
        power = bid

        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='post_dayAhead', EEG='False', typ='DEM'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'post_dayAhead')
        # Berechnung der Prognoseabweichung
        actual = np.asarray(self.portfolio.fixPlaning()/10**3, np.float).reshape((-1,))
        # Wenn kein Zuschlag am DayAhead Markt vorliegt, passe Nachfrage an
        difference = np.asarray([(schedule[i] - actual[i] if schedule[i] > 0 else 0.00) for i in self.portfolio.t])
        power = np.asarray([(actual[i] if schedule[i] > 0 else 0) for i in self.portfolio.t])
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
                    "tags": dict(agent=self.name, area=self.plz, timestamp='optimize_actual', EEG='False', typ='DEM'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Difference=difference[i], Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

    logging.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """ Abschlussplanung des Tages """
        power = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')  # Letzter bekannter  Fahrplan

        # Abspeichern der Ergebnisse
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='post_actual', EEG='False', typ='DEM'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        # Planung für den nächsten Tag
        # Anpassung der Prognosemethoden für den Verbrauch und die Preise
        for key, method in self.forecasts.items():
            if key != 'weather':
                method.collectData(self.date)
                method.counter += 1
                if method.counter >= method.collect:
                    method.fitFunction()
                    method.counter = 0

        logging.info('Tag %s abgeschlossen' % self.date)

        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = demAgent(date='2019-01-01', plz=args.plz, mongo=args.mongo, influx=args.influx,
                     market=args.market, dbName=args.dbName)
    agent.ConnectionMongo.login(agent.name, False)
    try:
        agent.run_agent()
    except Exception as e:
        logging.error('Fehler in run_agent: %s' %e)
    finally:
        agent.ConnectionInflux.influx.close()
        agent.ConnectionMongo.logout(agent.name)
        agent.ConnectionMongo.mongo.close()
        if agent.receive.is_open:
            agent.receive.close()
            agent.connection.close()
        exit()
