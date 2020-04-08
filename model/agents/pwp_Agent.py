import os
os.chdir(os.path.dirname(os.path.dirname(__file__)))
from aggregation.pwp_Port import pwpPort
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

class pwpAgent(basicAgent):

    def __init__(self, date, plz, mongo='149.201.88.150', influx='149.201.88.150', market='149.201.88.150', dbName='MAS_XXXX'):
        super().__init__(date=date, plz=plz, mongo=mongo, influx=influx, market=market, exchange='Market', typ='PWP', dbName=dbName)

        logging.info('Start des Agenten')

        # Aufbau des Portfolios mit den enstprechenden Kraftwerken und Speichern
        self.portfolio = pwpPort(typ='PWP', gurobi=True)                    # Verwendung von Gurobi

        # Einbindung der Kraftwerksdaten aus der MongoDB und dem TechFile in ./data
        data, tech = self.ConnectionMongo.getPowerPlants(plz)
        for i in range(len(data['power'])):
            name = 'plz_' + str(plz) + '_block_' + str(i)                   # Name des Kraftwerkblocks (1,...,n)
            fuel = data['fuel'][i]                                          # Brennstoff
            typ = data['typ'][i]                                            # Alter (typ)
            p = data['power'][i]                                            # Nennleistung [MW]
            t = tech[str(fuel) + '_%i' % typ]                               # weitere Daten wie Gradient, Out-Time, ...
            # Aufbau des Blocks
            block = {name: dict(
                        typ='konv',                                         # konventionelles Kraftwerk
                        fuel=fuel,                                          # Brennstoff
                        powerMax=np.round(p,1),                             # Maximalleistung
                        powerMin=np.round(p * t['out_min']/100, 1),         # Minimalleistung (Angabe in % in Tech)
                        eta=t['eta'],                                       # Wirkungsgrad
                        chi='0.15',                                         # Emissionsfaktor [kgCo2/MWh]
                        P0=np.round(p * t['out_min']/100, 1),               # aktuelle Leistung des Blocks
                        stopTime=t['down_min'],                             # minimale Stillstandszeit
                        runTime=t['up_min'],                                # minimale Laufzeit
                        on=t['up_min'],                                     # aktuelle Betriebsdauer
                        gradP=int(t['gradient']/100 * 4 * p),               # positiver Gradient (Angabe in %/15min in Tech)
                        gradM=int(t['gradient']/100 * 4 * p),               # negativer Gradient (Angabe in %/15min in Tech)
                        heat=[])                                            # mögliche Wärmelast
                    }
            self.portfolio.addToPortfolio(name, block)

        self.portfolio.pwpCap = sum(data['power'])                          # Gesamte Kraftwerksleitung  in [MW]

        logging.info('Kraftwerke hinzugefügt')

        # Einbindung der Speicherdaten aus der MongoDB
        data, tech = self.ConnectionMongo.getStorages(plz)
        for i in range(len(data['power'])):
            name = 'plz_' + str(plz) + '_storage_' + str(i)                 # Name des Speichers (1,...,n)
            power = data['power'][i]                                        # Nennleistung [MW]
            energy = data['energy'][i]                                      # Speicherkapazität [MWh]
            # Aufbau des Speichers
            storage = {name: {
                        'typ': 'storage',                                   # Speicherkraftwerk
                        'VMin': 0,                                          # Minimaler Speicherfüllstand
                        'VMax': energy,                                     # Maximaler Speicherfüllstand
                        'P+_Max': power,                                    # maximale Ladeleistung
                        'P-_Max': power,                                    # maximale Endladeleistung
                        'P+_Min': 0,                                        # minimale Ladeleistung
                        'P-_Min': 0,                                        # minimale EntLadeleistung
                        'V0': energy / 2,                                   # aktueller Speicherfüllstand
                        'eta+': 0.85,                                       # Ladewirkungsgrad
                        'eta-': 0.80 }                                      # Entladewirkungsgrad
                      }
            self.portfolio.addToPortfolio(name, storage)

        logging.info('Speicher hinzugefügt')

        # Parameter für die Handelsstrategie am Day Ahead Markt
        self.maxPrice = []                                                  # Maximalgebot
        self.minPrice = []                                                  # Minimalgenbot
        self.actions = np.zeros(24)                                         # Steigung der Gebotsgeraden für jede Stunde
        self.espilion = 0.8                                                 # Faktor zum Abtasten der Möglichkeiten
        self.lr = 0.8                                                       # Lernrate des Q-Learning-Einsatzes
        self.qLearn = daLearning(self.ConnectionInflux, init=5)             # Lernalgorithmus im 5 Tage Rythmus

        logging.info('Parameter der Handelsstrategie festgelegt')

        logging.info('Aufbau des Agenten abgeschlossen')

    def optimize_balancing(self):
        # TODO Überarbeiten der Gebotsstrategie --> Biete immer 30 % der Verfügbaren Menge zu Random Preisen
        """Einsatzplanung für den Regelleistungsmarkt"""
        orderbook = dict()                                                  # Oderbook für alle Blöcke (Stunde 1-6)

        states = [0, 0]
        for _, value in self.portfolio.energySystems.items():
            if value['typ'] == 'konv':
                if value['P0'] == 0:
                    states[0] += 0
                    states[1] += 0
                else:
                    states[0] += min(value['powerMax'] - value['P0'], value['gradP'])
                    states[1] += max(min(value['P0'] - value['powerMin'], value['gradM']), 0)

        for i in range(6):
            a = 0.3
            powerPricePos = np.random.uniform(low=100, high=500)
            powerPriceNeg = np.random.uniform(low=100, high=500)
            energyPricePos = np.random.uniform(low=0, high=50)
            energyPriceNeg = np.random.uniform(low=0, high=50)
            orderbook.update({'neg_%s' % i: {'quantity': np.round(states[1] * a, 0), 'powerPrice': powerPriceNeg,
                                             'energyPrice': energyPriceNeg, 'typ': 'neg', 'slot': i, 'name': self.name}})
            orderbook.update({'pos_%s' % i: {'quantity': np.round(states[0] * a, 0), 'powerPrice': powerPricePos,
                                             'energyPrice': energyPricePos, 'typ': 'pos', 'slot': i, 'name': self.name}})

        self.ConnectionMongo.setBalancing(self.name, self.date, orderbook)

        logging.info('Planung Regelleistungsmarkt abgeschlossen')

    def optimize_dayAhead(self):
        """Einsatzplanung für den DayAhead-Markt"""
        orderbook = dict()                                                  # Oderbook für alle Gebote (Stunde 1-24)
        # Prognosen für den kommenden Tag
        weather = self.weatherForecast()                                    # Wetterdaten (dir,dif,temp,wind)
        price = self.priceForecast()                                        # Preisdaten (power,gas,nuc,coal,lignite)
        demand = self.demandForecast()                                      # Lastprognose
        # Verpflichtungen Regelleistung
        pos, neg = self.ConnectionInflux.getBalancingPower(self.date, self.name)
        self.portfolio.setPara(self.date, weather,  price, demand, pos, neg)
        self.portfolio.buildModel()
        power = np.asarray(self.portfolio.optimize(), np.float)             # Berechnung der Einspeiseleitung

        self.portfolio.buildModel(max_=True)
        powerMax = np.asarray(self.portfolio.optimize(), np.float)
        powerMax = powerMax - power
        E = np.asarray([np.round(self.portfolio.m.getVarByName('E[%i]' % i).x, 2) for i in self.portfolio.t])
        F = np.asarray([np.round(self.portfolio.m.getVarByName('F[%i]' % i).x, 2) for i in self.portfolio.t])
        priceMax = (E+F) + 1.5

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
        var = 2

        if len(self.forecasts['price'].mcp):
            var = np.sqrt(np.var(self.forecasts['price'].mcp) * self.forecasts['price'].factor)

        self.maxPrice = prc.reshape((-1,)) + var
        self.minPrice = prc.reshape((-1,)) - var
        delta = self.maxPrice - self.minPrice
        slopes = (delta/100) * np.tan((slopes+10)/180*np.pi)   # Preissteigung pro weitere MW

        # Füge für jede Stunde die entsprechenden Gebote hinzu
        for i in range(self.portfolio.T):
            # biete immer den minimalen Preis, aber nie mehr als den maximalen Preis
            quantity = [-1*(20/100 * power[i]) for _ in range(20, 120, 20)]
            quantity.append(powerMax[i])
            price = [float(max(slopes[i] * p + self.minPrice[i], self.maxPrice[i])) for p in range(20, 120, 20)]
            price.append(priceMax[i])
            orderbook.update({'h_%s' % i: {'quantity': quantity, 'price': price, 'hour': i, 'name': self.name}})

        self.ConnectionMongo.setDayAhead(name=self.name, date=self.date, orders=orderbook)

        json_body = []
        for key, value in self.portfolio.energySystems.items():
            time = self.date
            power = [self.portfolio.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            volume = np.zeros_like(power)
            if value['typ'] == 'storage':
                volume = [self.portfolio.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.portfolio.t]
            for i in self.portfolio.t:
                json_body.append(
                    {
                        "measurement": 'Areas',
                        "tags": dict(plant=value['typ'], asset=key, agent=self.name, area=self.area,
                                     timestamp='optimize_dayAhead', typ='PWP'),
                        "time": time.isoformat() + 'Z',
                        "fields": dict(Power=power[i], Volume=volume[i])
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
        profit = np.asarray([(ask[i]-bid[i])*price[i] for i in range(24)])

        # Minimiere Differenz zu den bezuschlagten Geboten
        self.portfolio.buildModel(response=ask-bid)
        power = self.portfolio.fixPlaning()
        E = np.asarray([np.round(self.portfolio.m.getVarByName('E[%i]' % i).x, 2) for i in self.portfolio.t])
        F = np.asarray([np.round(self.portfolio.m.getVarByName('F[%i]' % i).x, 2) for i in self.portfolio.t])
        costs = E+F
        profit = profit.reshape((24,)) - costs.reshape((24,))
        # Falls ein Modell des Energiesystems vorliegt, passe die Gewinnerwartung entsprechend der Lernrate an
        if self.qLearn.fitted:
            states = self.qLearn.getStates(self.date)
            for i in self.portfolio.t:
                oldValue = self.qLearn.qus[states[i], int(self.actions[i]-10)]
                self.qLearn.qus[states[i], int(self.actions[i]-10)] = oldValue + self.lr * (profit[i] - oldValue)

        # Abspeichern der Ergebnisse
        json_body = []
        time = self.date
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_dayAhead', typ='PWP'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Power=power[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

        logging.info('DayAhead Ergebnisse erhalten')

    def optimize_actual(self):
        """Abruf Prognoseabweichung und Übermittlung der Fahrplanabweichung"""
        ask = self.ConnectionInflux.getDayAheadAsk(self.date, self.name)
        bid = self.ConnectionInflux.getDayAheadBid(self.date, self.name)
        schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'post_dayAhead')

        # Berechnung der Abweichung, die zum Abruf von Regelenergie führt
        difference = schedule-(ask-bid)
        # Aufbau der "Gebote" (Abweichungen zum gemeldeten Fahrplan)
        orderbook = dict()
        for i in range(self.portfolio.T):
            orderbook.update({'h_%s' % i: {'quantity': difference[i], 'hour': i, 'name': self.name}})
        self.ConnectionMongo.setActuals(name=self.name, date=self.date, orders=orderbook)
        # Abspeichern der Ergebnisse
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='optimize_actual', typ='PWP'),
                    "time": time.isoformat() + 'Z',
                    "fields": dict(Difference=difference[i], Power=schedule[i])
                }
            )
            time = time + pd.DateOffset(hours=self.portfolio.dt)
        self.ConnectionInflux.saveData(json_body)

    logging.info('Aktuellen Fahrplan erstellt')

    def post_actual(self):
        """Abschlussplanung des Tages"""
        pos, neg = self.ConnectionInflux.getBalancingEnergy(self.date,self.name)
        self.portfolio.setPara(self.date, self.weatherForecast(), self.priceForecast(), self.demandForecast(),
                               np.zeros(self.portfolio.T), np.zeros(self.portfolio.T))
        schedule = self.ConnectionInflux.getPowerScheduling(self.date, self.name, 'optimize_actual')
        self.portfolio.buildModel(response=schedule + pos - neg)
        power = self.portfolio.fixPlaning()

        # -- save result
        time = self.date
        json_body = []
        for i in self.portfolio.t:
            json_body.append(
                {
                    "measurement": 'Areas',
                    "tags": dict(agent=self.name, area=self.area, timestamp='post_actual', typ='PWP'),
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
    agent = pwpAgent(date='2019-01-01', plz=args.plz, mongo=args.mongo, influx=args.influx,
                     market=args.market, dbName=args.dbName)
    agent.ConnectionMongo.login(agent.name, True)
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
