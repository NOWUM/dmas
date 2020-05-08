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
    parser.add_argument('--plz', type=int, required=False, default=35, help='PLZ-Agent')
    parser.add_argument('--mongo', type=str, required=False, default='149.201.88.150', help='IP MongoDB')
    parser.add_argument('--influx', type=str, required=False, default='149.201.88.150', help='IP InfluxDB')
    parser.add_argument('--market', type=str, required=False, default='149.201.88.150', help='IP Market')
    parser.add_argument('--dbName', type=str, required=False, default='MAS_XXXX', help='Name der Datenbank')
    return parser.parse_args()

class resAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='RES', dbName=dbName)

        logging.info('Start des Agenten')
        # Aufbau des Portfolios mit den enstprechenden EE-Anlagen
        self.portfolio = resPort(typ='RES', gurobi=False)                    # Keine Verwendung eines Solvers

        # Einbindung der Solardaten aus der MongoDB (EEG Anlagen vor 2013)
        solarsystems = self.ConnectionMongo.getPvEEG(plz)
        total = 0
        for key, system in solarsystems.items():
            total += system['maxPower']
        self.portfolio.capacities['solar'] = np.round(total / 1000, 2)
        name = 'EEG_SolarSystem_%s' % plz
        generator = dict(maxPower=np.round(total, 2), eta=0.15, area=7, typ='solarsystem', fuel='solar')
        self.portfolio.addToPortfolio(name, {name: generator})

        logging.info('PV(EEG)-Erzeugung hinzugefügt')

        # Einbindung der Laufwasserkraftwerke aus der MongoDB
        runRiver = self.ConnectionMongo.getRunRiver(plz)
        total = 0
        for key, system in runRiver.items():
            total += system['maxPower']
        self.portfolio.capacities['water'] = np.round(total / 1000, 2)
        name = 'EEG_RunRiver_%s' % plz
        generator = dict(maxPower=np.round(total, 2), typ='run-river', fuel='water')
        self.portfolio.addToPortfolio(name, {name: generator})

        logging.info('Laufwasserkraftwerk (EEG) hinzugefügt')

        # Einbindung der Biomasseanlagen aus der MongoDB
        bioMass = self.ConnectionMongo.getBioMass(plz)
        total = 0
        for key, system in bioMass.items():
            total += system['maxPower']
        self.portfolio.capacities['bio'] = np.round(total / 1000, 2)
        name = 'EEG_BioMass_%s' % plz
        generator = dict(maxPower=np.round(total, 2), typ='biomass', fuel='bio')
        self.portfolio.addToPortfolio(name, {name: generator})

        logging.info('Biomassekraftwerke (EEG) hinzugefügt')

        # Einbindung der Winddaten aus der MongoDB
        windOnshore = self.ConnectionMongo.getWindOn(plz)
        total = 0
        for key, system in windOnshore.items():
            self.portfolio.addToPortfolio(key, {key: system})
            total += system['maxPower']
        self.portfolio.capacities['wind'] = np.round(total / 1000, 2)                   # Gesamte Windleistung in [MW]

        logging.info('Winderzeugung hinzugefügt')

        # Einbindung der Solardaten aus der MongoDB (nur PV-Anlagen > 750 kW)
        solarparks = self.ConnectionMongo.getPvParks(plz)
        total = 0
        for key, system in solarparks.items():
            self.portfolio.addToPortfolio(key, {key: system})
            total += system['maxPower']
        self.portfolio.capacities['solar'] += np.round(total / 1000, 2)         # Gesamte Solarleistung in [MW]

        logging.info('PV-Erzeugung hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = 10
        self.minPrice = 0.1
        self.actions = np.zeros(24)                                         # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.5                                                 # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                       # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))             # Lernalgorithmus im x Tage Rythmus
        self.qLearn.qus[:, 0] = self.qLearn.qus[:, 0] * (self.portfolio.capacities['wind']
                                                      + self.portfolio.capacities['solar'])

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

        # --> Dirketvermakrtung <--
        self.portfolio.setPara(self.date, weather, price, demand)
        self.portfolio.buildModel()
        _ = np.asarray(self.portfolio.optimize(), np.float)       # Berechnung der Einspeiseleitung

        powerDirect = []
        powerEEG = []
        for key, value in agent.portfolio.energySystems.items():
            if value['typ'] == 'wind' or value['typ'] == 'solarpark':
                powerDirect.append(value['model'].generation['wind'])
                powerDirect.append(value['model'].generation['solar'])
            else:
                powerEEG.append(value['model'].generation['bio'])
                powerEEG.append(value['model'].generation['water'])
                powerEEG.append(value['model'].generation['solar'])
        if len(powerDirect) == 0:
            powerDirect = np.zeros(24)
        else:
            powerDirect = np.sum(np.asarray(powerDirect), axis=0)
        if len(powerEEG) == 0:
            powerEEG = np.np.zeros(24)
        else:
            powerEEG = np.sum(np.asarray(powerEEG), axis=0)

        # Aufbau der linearen Gebotskurven
        slopes = np.random.randint(1, 8, 24) * 10
        wnd = np.asarray(weather['wind']).reshape((-1, 1))                  # Wind [m/s]
        rad = np.asarray(weather['dir']).reshape((-1, 1))                   # Dirkete Strahlung [W/m²]
        tmp = np.asarray(weather['temp']).reshape((-1, 1))                  # Temperatur [°C]
        dem = np.asarray(demand).reshape((-1, 1))                           # Lastprognose [MW]
        prc = np.asarray(price['power']).reshape((-1, 1))                   # MCP Porgnose
        if self.qLearn.fitted and (self.espilion > np.random.uniform(0,1)):
            # Wenn ein Modell vorliegt und keine neuen Möglichkeiten ausprobiert werden sollen
            slopes = self.qLearn.getAction(wnd, rad, tmp, dem, prc)

        self.actions = slopes                                               # abschpeichern der Ergebnissek
        var = np.sqrt(np.var(self.forecasts['price'].y) * self.forecasts['price'].factor)

        self.maxPrice = prc.reshape((-1,)) - max(2*var, 1)
        self.minPrice = np.zeros_like(self.maxPrice)

        delta = self.maxPrice - self.minPrice
        slopes = (delta/100) * np.tan((slopes+10)/180*np.pi)   # Preissteigung pro weitere MW

        # Füge für jede Stunde die entsprechenden Gebote hinzu
        for i in range(self.portfolio.T):
            # biete immer den minimalen Preis, aber nie mehr als den maximalen Preis
            quantity = [-1*powerEEG[i]]
            for _ in range(2, 102, 2):
                quantity.append(-1*(2/100 * powerDirect[i]))
            price = [-499.98]
            for p in range(2, 102, 2):
                price.append(float(min(slopes[i] * p + self.minPrice[i], self.maxPrice[i])))
            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            for key, value in self.portfolio.energySystems.items():
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(agent=self.name, area=self.plz, timestamp='optimize_dayAhead', typ='RES', asset=key, fuel=value['fuel']),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=value['model'].power[i])
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
        planing = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_dayAhead')
        difference = np.asarray(planing).reshape((-1,)) - np.asarray(power).reshape((-1,))
        missed = [difference[i]*price[i] if price[i] > 0 else 0 for i in range(24)]
        # Falls ein Modell des Energiesystems vorliegt, passe die Gewinnerwartung entsprechend der Lernrate an
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in self.portfolio.t:
                oldValue = self.qLearn.qus[states[i], int((self.actions[i]-10)/10)]
                self.qLearn.qus[states[i], int((self.actions[i]-10)/10)] = oldValue + self.lr * (profit[i] - missed[i] - oldValue)

       # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.plz, timestamp='post_dayAhead', typ='RES'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i], Delta=difference[i])
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
                    "tags": dict(agent=self.name, area=self.plz, timestamp='optimize_actual', typ='RES', EEG='False'),
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
                    "tags": dict(agent=self.name, area=self.plz, timestamp='post_actual', typ='RES'),
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

            self.lr = max(self.lr*0.999, 0.4)                                # Lernrate * 0.999 (Annahme Markt ändert sich
                                                                             # Zukunft nicht mehr so schnell)
            self.espilion = max(0.999*self.espilion, 0.1)                    # Epsilion * 0.999 (mit steigender Simulationdauer
                                                                             # sind viele Bereiche schon bekannt
            print(self.lr)
            print(self.espilion)

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
