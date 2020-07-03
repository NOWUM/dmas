import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.res_Port import resPort
from agents.basic_Agent import agent as basicAgent
from apps.qLearn_DayAhead import qLeran as daLearning
import argparse
import pandas as pd
import numpy as np


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--plz', type=int, required=False, default=35, help='PLZ-Agent')
    return parser.parse_args()

class resAgent(basicAgent):

    def __init__(self, date, plz):
        super().__init__(date=date, plz=plz, exchange='Market', typ='RES')
        # Aufbau des Portfolios mit den enstprechenden EE-Anlagen
        self.logger.info('Start des Agenten')
        self.portfolio = resPort(typ='RES')

        # Aufbau der Freiflächen PV und gewerblich genutzten Anlagen
        for key, value in self.ConnectionMongo.getPvParks().items():
            self.portfolio.capacities['solar'] += value['maxPower']
            self.portfolio.addToPortfolio(key, {key: value})
        self.logger.info('PV(Gewerbe)-Erzeugung hinzugefügt')

        # Aufbau der EEG vergüteten PV Dachanlgen
        for key, value in self.ConnectionMongo.getPVs().items():
            self.portfolio.capacities['solar'] += value['PV']['maxPower'] * value['EEG']
            self.portfolio.addToPortfolio('Pv' + str(key), {'Pv' + str(key): value})
        self.logger.info('PV(Haushalte)-Erzeugung hinzugefügt')

        # Einbindung der Laufwasserkraftwerke
        for key, value in self.ConnectionMongo.getRunRiver().items():
            self.portfolio.addToPortfolio('runRiver', {'runRiver': value})
            self.portfolio.capacities['water'] = value['maxPower']
        self.logger.info('Laufwasserkraftwerke hinzugefügt')

        # Einbindung der Biomassekraftwerke
        for key, value in self.ConnectionMongo.getBioMass().items():
            self.portfolio.addToPortfolio('bioMass', {'bioMass': value})
            self.portfolio.capacities['bio'] = value['maxPower']
        self.logger.info('Biomassekraftwerke hinzugefügt')

        # Einbindung der Winddaten aus der MongoDB
        #windOnshore = self.ConnectionMongo.getWindOn(plz)
        #total = 0
        #for key, system in windOnshore.items():
        #    self.portfolio.addToPortfolio(key, {key: system})
        #    total += system['maxPower']
        #self.portfolio.capacities['wind'] = np.round(total / 1000, 2)

        self.logger.info('Winderzeugung hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = np.zeros(24)                                                        # Maximalgebote
        self.minPrice = np.zeros(24)                                                        # Minimalgebote
        self.actions = np.zeros(24)                                                         # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.3                                                                 # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                                       # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=np.random.randint(5, 10 + 1))  # Lernalgorithmus im x Tage Rythmus
        self.qLearn.qus[:, 0] = self.qLearn.qus[:, 0] * (self.portfolio.capacities['wind']
                                                      + self.portfolio.capacities['solar'])

        self.risk = np.random.choice([-3, -2, -1, 0, 1, 2, 3])

        self.logger.info('Parameter der Handelsstrategie festgelegt')

        if len(self.portfolio.energySystems) == 0:
            self.logger.info('Keine Energiesysteme im PLZ-Gebiet vorhanden')
            exit()

        self.logger.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        """Einsatzplanung für den Regelleistungsmarkt"""
        self.logger.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()                                                  # Oderbook für alle Gebote (Stunde 1-24)
        json_body = []                                                      # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Prognosen für den kommenden Tag
        weather = self.weatherForecast(self.date)                           # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast(self.date)                               # Preisdaten (power,gas,nuc,coal,lignite)
        demand = self.demandForecast(self.date)                             # Lastprognose

        # Standardoptimierung
        self.portfolio.setPara(self.date, weather, price, demand)
        self.portfolio.buildModel()
        power_dayAhead = np.asarray(self.portfolio.optimize(), np.float)    # Berechnung der Einspeiseleitung

        powerDirect = np.zeros(24)
        powerEEG = np.zeros(24)
        for key, value in agent.portfolio.energySystems.items():
            # direkt-vermarktete Leistung
            if value['typ'] == 'wind' or value['typ'] == 'biomass' or value['typ']=='PVPark':
                powerDirect += value['model'].generation['wind'].reshape(-1)            # Wind Onshore
                powerDirect += value['model'].generation['solar'].reshape(-1)           # Freiflächen PV
                powerDirect += value['model'].generation['bio'].reshape(-1)             # Biomasse-Kraftwerk
            # EEG-vermarktete Leistung
            else:
                powerEEG += value['model'].generation['water'].reshape(-1)              # Laufwasser-Kraftwerk
                powerEEG += value['model'].generation['solar'].reshape(-1)              # PV-Anlage vor 2013

        # Portfolioinformationen
        time = self.date                                                                # Zeitstempel = aktueller Tag
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='RES',                                             # Typ Erneuerbare Energien
                                 agent=self.name,                                       # Name des Agenten
                                 area=self.plz,                                         # Plz Gebiet
                                 timestamp='optimize_dayAhead'),                        # Zeitstempel der Tagesplanung
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power_dayAhead[i],                        # gesamte geplante Einspeisung  [MW]
                                   priceForcast=price['power'][i],                      # Day Ahead Preisprognose       [€/MWh]
                                   powerWind=self.portfolio.generation['wind'][i],      # gesamte Windeinspeisung       [MW]
                                   powerBio=self.portfolio.generation['bio'][i],        # gesamte Biomasseeinspeisung   [MW]
                                   powerSolar=self.portfolio.generation['solar'][i],    # gesamte PV-Einspeisung        [MW]
                                   powerWater=self.portfolio.generation['water'][i],    # gesamte Wasserkraft           [MW]
                                   powerDirect=powerDirect[i],                          # direkt-vermarktete Leistung   [MW]
                                   powerEEG=powerEEG[i])                                # EEG-vermarktete Leistung      [MW]
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        # Aufbau der linearen Gebotskurven
        actions = np.random.randint(1, 8, 24) * 10
        prc = np.asarray(price['power']).reshape((-1, 1))                               # MCP Porgnose      [€/MWh]

        # Wenn ein Modell vorliegt und keine neuen Möglichkeiten ausprobiert werden sollen
        if self.qLearn.fitted and (self.espilion > np.random.uniform(0, 1)):
            wnd = np.asarray(weather['wind']).reshape((-1, 1))                          # Wind              [m/s]
            rad = np.asarray(weather['dir']).reshape((-1, 1))                           # Dirkete Strahlung [W/m²]
            tmp = np.asarray(weather['temp']).reshape((-1, 1))                          # Temperatur        [°C]
            dem = np.asarray(demand).reshape((-1, 1))                                   # Lastprognose      [MW]
            actions = self.qLearn.getAction(wnd, rad, tmp, dem, prc)

        self.actions = actions                                                          # abschpeichern der Aktionen

        # Berechnung der Prognosegüte
        var = np.sqrt(np.var(self.forecasts['price'].mcp, axis=0) * self.forecasts['price'].factor)
        var = np.nan_to_num(var)

        self.maxPrice = prc.reshape((-1,)) + np.asarray([max(self.risk*v, 1) for v in var])   # Maximalpreis      [€/MWh]
        self.minPrice = np.zeros_like(self.maxPrice)                                          # Minimalpreis      [€/MWh]

        slopes = ((self.maxPrice - self.minPrice)/100) * np.tan((actions+10)/180*np.pi) # Preissteigung pro weitere MW

        # Füge für jede Stunde die entsprechenden Gebote hinzu
        for i in range(self.portfolio.T):
            quantity = [-1*powerEEG[i]]
            price = [-499.98]
            for _ in range(2, 102, 2):
                quantity.append(-1*(2/100 * powerDirect[i]))

            ub = self.maxPrice[i]
            lb = self.minPrice[i]
            slope = slopes[i]

            if slope > 0:
                for p in range(2, 102, 2):
                    price.append(float(min(slope * p + lb, ub)))
            else:
                for p in range(2, 102, 2):
                    price.append(float(min(-1*slope * p + ub, lb)))

            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)
        self.logger.info('Planung DayAhead-Markt abgeschlossen')

    def post_dayAhead(self):
        """Reaktion auf  die DayAhead-Ergebnisse"""
        json_body = []                                                          # Liste zur Speicherung der Ergebnisse in der InfluxDB

        # Speichern der Daten und Aktionen, um aus diesen zu lernen
        self.qLearn.collectData(self.date, self.actions.reshape((24, 1)))
        # geplante Menge Day Ahead
        planing = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_dayAhead')

        # Abfrage der DayAhead Ergebnisse
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)            # Angebotene Menge [MWh]
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)            # Nachgefragte Menge [MWh]
        price = self.ConnectionInflux.getDayAheadPrice(self.date)                   # MCP [€/MWh]
        profit = [float((ask[i] - bid[i]) * price[i]) for i in range(24)]           # erzielte Erlöse

        # Differenz aus Planung und Ergebnissen
        difference = np.asarray(planing).reshape((-1,)) - np.asarray(ask - bid).reshape((-1,))

        # Bestrafe eine nicht Vermarktung
        missed = [difference[i]*price[i] if price[i] > 0 else 0 for i in range(24)]

        # Falls ein Modell vorliegt, passe die Gewinnerwartung entsprechend der Lernrate an
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in self.portfolio.t:
                oldValue = self.qLearn.qus[states[i], int((self.actions[i]-10)/10)]
                self.qLearn.qus[states[i], int((self.actions[i]-10)/10)] = oldValue + self.lr * (profit[i] - missed[i] - oldValue)
        else:
            states = [-1 for _ in self.portfolio.t]

        # Portfolioinformation
        time = self.date                                                                # Zeitstempel
        power = np.asarray(ask - bid).reshape((-1,))
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(typ='RES',                                             # Typ Erneuerbare Energien
                                 agent=self.name,                                       # Name des Agenten
                                 area=self.plz,                                         # Plz Gebiet
                                 timestamp='post_dayAhead'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(powerTotal=power[i],                                 # gesamte Einspeisung           [MW]
                                   powerWind=self.portfolio.generation['wind'][i],      # gesamte Windeinspeisung       [MW]
                                   powerBio=self.portfolio.generation['bio'][i],        # gesamte Biomasseeinspeisung   [MW]
                                   powerSolar=self.portfolio.generation['solar'][i],    # gesamte PV-Einspeisung        [MW]
                                   powerWater=self.portfolio.generation['water'][i],    # gesamte Wasserkraft           [MW]
                                   profit=profit[i],                                    # erzielte Erlöse               [€]
                                   state=int(states[i]),
                                   action=int((self.actions[i] - 10) / 10))
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)

        self.ConnectionInflux.saveData(json_body)

        self.logger.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        # TODO: Überarbeitung, wenn Regelleistung
        self.logger.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        # TODO: Überarbeitung, wenn Regelleistung
        # Planung für den nächsten Tag
        # Anpassung der Prognosemethoden für den Verbrauch und die Preise
        if self.delay <= 0:
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
        else:
            self.delay -= 1

        self.logger.info('Tag %s abgeschlossen' %self.date)
        print('Agent %s %s done' % (self.name, self.date.date()))

if __name__ == "__main__":

    args = parse_args()
    agent = resAgent(date='2019-01-01', plz=args.plz)
    agent.ConnectionMongo.login(agent.name, False)
    # try:
    #     agent.run_agent()
    # except Exception as e:
    #     print(e)
    # finally:
    #     agent.ConnectionInflux.influx.close()
    #     agent.ConnectionMongo.logout(agent.name)
    #     agent.ConnectionMongo.mongo.close()
    #     if agent.receive.is_open:
    #         agent.receive.close()
    #         agent.connection.close()
    #     exit()
