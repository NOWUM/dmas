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
    parser.add_argument('--mongo', type=str, required=False, default='127.0.0.1', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='127.0.0.1', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='127.0.0.1', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_XXXX', help='Name der Datenbank')
    return parser.parse_args()

class resAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='RES', dbName=dbName)

        logging.info('Start des Agenten')

        # Aufbau des Portfolios mit den enstprechenden Wind- und Solaranlagen
        self.portfolio = resPort(typ='RES', gurobi=False)                   # Keine Verwendung eines Solvers

        # Einbindung der Windaten aus der MongoDB und dem TechFile in ./data
        data, tech = self.ConnectionMongo.getWindOn(plz)
        for i in range(len(data['power'])):
            name = 'plz_' + str(plz) + '_windOn_' + str(i)                  # Name der Windkraftanalge (1,..,n)
            generator = dict(P=data['power'][i], typ='wind')                # Nennleistung [kW]
            generator.update(tech[str(data['typ'][i])])                     # Daten der Einspeisekurve
            self.portfolio.addToPortfolio(name, {name : generator})
        self.portfolio.Cap_Wind = sum(data['power']) / 1000                 # Gesamte Windleistung in [MW]

        logging.info('Winderzeugung hinzugefügt')

        # Einbindung der Solardaten aus der MongoDB (nur PV-Anlagen > 750 kW)
        data, tech = self.ConnectionMongo.getPvParks(plz)
        for i in range(len(data['power'])):
            name = 'plz_' + str(plz) + '_solar_' + str(i)                   # Name der PV-Parks (1,..,n)
            generator = dict(peakpower=data['power'][i],                    # Default Werte sowie die angegebene
                             typ='solar', eta=0.15, area=3.5)               # Nennleistung [kW]
            self.portfolio.addToPortfolio(name, {name : generator})
        self.portfolio.Cap_Solar = sum(data['power']) / 1000                # Gesamte Solarleistung in [MW]

        logging.info('PV-Erzeugung hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = 10                                                  # Maximalgebot entspricht dem 10fachen MCP
        self.minPrice = 0.1                                                 # Minimalgenbot entspricht dem 0.1fachen MCP
        self.actions = np.zeros(24)                                         # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.8                                                 # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                       # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=5)             # Lernalgorithmus im 5 Tage Rythmus

        logging.info('Parameter der Handelsstrategie festgelegt')

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
        demand = self.demandForecast()                                      # Lastprognose
        self.portfolio.setPara(self.date, weather, price, demand)
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)             # Berechnung der Einspeiseleitung

        # Aufbau der linearen Gebotskurven
        slopes = np.random.randint(10, 80, 24)
        wnd = np.asarray(weather['wind']).reshape((-1, 1))                  # Wind [m/s]
        rad = np.asarray(weather['dir']).reshape((-1, 1))                   # Dirkete Strahlung [W/m²]
        tmp = np.asarray(weather['temp']).reshape((-1, 1))                  # Temperatur [°C]
        dem = np.asarray(demand).reshape((-1, 1))                           # Lastprognose [MW]
        prc = np.asarray(price['power']).reshape((-1, 1))                   # MCP Porgnose
        if self.qLearn.fitted and (self.espilion > np.random.uniform(0,1)):
            # Wenn ein Modell vorliegt und keine neuen Möglichkeiten ausprobiert werden sollen
            slopes = self.qLearn.getAction(wnd, rad, tmp, dem, prc)

        self.actions = slopes                                               # abschpeichern der Ergebnisse
        slopes = (prc.reshape((-1,))/100) * np.tan((slopes+10)/180*np.pi)   # Preissteigung pro weitere MW
        # Füge für jede Stunde die entsprechenden Gebote hinzu
        for i in range(self.portfolio.T):
            # biete immer den minimalen Preis, aber nie mehr als den maximalen Preis
            quantity = [-1*(20/100 * power[i]) for _ in range(20, 120, 20)]
            price = [float(min(self.maxPrice * prc[i], max(self.minPrice * prc[i], slopes[i] * p))) for p in range(20, 120, 20)]
            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name':self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='optimize_dayAhead', typ='RES'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i], Wind=self.portfolio.pWind[i], Solar=self.portfolio.pSolar[i])
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

        # Falls ein Modell des Energiesystems vorliegt, passe die Gewinnerwartung entsprechend der Lernrate an
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in self.portfolio.t:
                oldValue = self.qLearn.qus[states[i], int(self.actions[i]-10)]
                self.qLearn.qus[states[i], int(self.actions[i]-10)] = oldValue + self.lr * (profit[i] - oldValue)

        power = ask - bid

       # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_dayAhead', typ='RES'),
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
        actual = self.portfolio.fixPlaning()                    # Berechung des aktuellen Fahrplans + Errors
        # Wenn kein Zuschlag am DayAhead Markt vorliegt, regel die Anlage runter
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
                    "tags": dict(agent=self.name, area=self.area, timestamp='optimize_actual', typ='RES'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i], Difference=difference[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        power = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')     # Letzter bekannter  Fahrplan

        # Abspeichern der Ergebnisse
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_actual', typ='RES'),
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

        # Ansappung der Statuspunkte des Energiesystems
        self.qLearn.counter += 1
        if self.qLearn.counter >= self.qLearn.collect:
            self.qLearn.fit()
            self.qLearn.counter = 0

        self.lr = max(self.lr*0.9, 0.4)                                 # Lernrate * 0.9 (Annahme Markt ändert sich
                                                                        # Zukunft nicht mehr so schnell)
        self.espilion = max(0.9*self.espilion, 0.2)                     # Epsilion * 0.9 (mit steigender Simulationdauer
                                                                        # sind viele Bereiche schon bekannt

        logging.info('Tag %s abgeschlossen' %self.date)
        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = resAgent(date='2019-01-01', plz=args.plz, mongo=args.mongo, influx=args.influx,
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
